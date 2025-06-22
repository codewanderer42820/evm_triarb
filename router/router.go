// ✅ Tuned for max performance: zero alloc, cache-aligned, L1-friendly
// ✅ Core identity (Fwd or Rev) is now configurable per runLoop instance
// ✅ SWAR-style recompute loop added
// ✅ Fully implemented: mustDB, parseCycles, wireAddrs, buildSplit

package main

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"log"
	"math/rand"
	"os"
	"runtime"
	"time"
	"unsafe"

	_ "github.com/mattn/go-sqlite3"

	"main/bucketqueue"
	"main/pairidx"
	"main/ring"
)

var (
	routers []*CoreRouter
	rings   []*ring.Ring
)

type DeltaBucket struct {
	Queue  *bucketqueue.Queue
	CurLog float64
	//lint:ignore U1000 keep struct 64 B-aligned
	_pad [48]byte
}

type ArbPath struct {
	LegVal [3]float64
	Addr   [3][20]byte
	Dir    [3]bool
	_pad   byte
}

type ref struct {
	H bucketqueue.Handle
	Q *bucketqueue.Queue
}

type PriceUpdate struct {
	FwdTick float64
	RevTick float64
	Addr    [20]byte
	_pad    [4]byte
}

type CoreRouter struct {
	bucketByKey *pairidx.HashMap
	queueByKey  *pairidx.HashMap
	buckets     []*DeltaBucket
	fanOut      [][]ref
}

type Cycle struct {
	pools [3]int
	last  int
	ap    *ArbPath
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	n := runtime.NumCPU() - 2
	if n < 2 {
		log.Fatal("need ≥ 2 CPUs")
	}

	routers = make([]*CoreRouter, n)
	rings = make([]*ring.Ring, n)
	for i := 0; i < n; i++ {
		routers[i] = &CoreRouter{
			bucketByKey: pairidx.New(),
			queueByKey:  pairidx.New(),
		}
		rings[i] = ring.New(1 << 14)
		isRev := (i >= n/2)
		go runLoop(i, rings[i], routers[i], isRev)
	}

	db := mustDB("uniswap_pairs.db")
	rand.Seed(time.Now().UnixNano())
	pairs := parseCycles("cycles_3_3.txt")
	bucketTable := map[[20]byte][2]*DeltaBucket{}

	for i := range pairs {
		wireAddrs(db, &pairs[i])
		buildSplit(bucketTable, &pairs[i])
	}

	select {}
}

func runLoop(id int, in *ring.Ring, rt *CoreRouter, isRev bool) {
	var stop, hot uint32
	ring.PinnedConsumer(id, in, &stop, &hot, func(ptr unsafe.Pointer) {
		u := (*PriceUpdate)(ptr)
		addrPtr := (*[20]byte)(unsafe.Pointer(&u.Addr[0]))
		key := *addrPtr

		val := u.FwdTick
		if isRev {
			val = u.RevTick
		}

		if i, ok := rt.bucketByKey.Get(string(key[:])); ok {
			rt.buckets[i].CurLog = val
		}
		if fi, ok := rt.queueByKey.Get(string(key[:])); ok {
			recomputeSWAR(rt.fanOut[fi])
		}
	}, nil)
}

func recomputeSWAR(refs []ref) {
	const unroll = 4
	var aps [unroll]*ArbPath
	for i := 0; i < len(refs); i += unroll {
		for j := 0; j < unroll && i+j < len(refs); j++ {
			r := refs[i+j]
			aps[j] = (*ArbPath)(unsafe.Pointer(&r.Q.Arena[r.H]))
		}
		for j := 0; j < unroll && i+j < len(refs); j++ {
			tick := aps[j].LegVal[1] + aps[j].LegVal[2]
			_ = refs[i+j].Q.Update(int64(tick*1e6), refs[i+j].H)
		}
	}
}

func mustDB(path string) *sql.DB {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatalf("failed to open DB: %v", err)
	}
	return db
}

func parseCycles(path string) []Cycle {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("parseCycles: %v", err)
	}
	defer f.Close()

	var out []Cycle
	sc := bufio.NewScanner(f)
	var buf [64]byte

	for sc.Scan() {
		line := sc.Bytes()
		n := 0
		var tmp [4]int
		i := 0
		for _, c := range line {
			if c >= '0' && c <= '9' {
				buf[n] = c
				n++
				continue
			}
			if n > 0 && i < 4 {
				tmp[i] = fastAtoi(buf[:n])
				n = 0
				i++
			}
			if i == 4 {
				break
			}
		}
		if i < 3 {
			continue
		}
		c := Cycle{pools: [3]int{tmp[0], tmp[1], tmp[2]}, last: tmp[3], ap: &ArbPath{}}
		out = append(out, c)
	}
	return out
}

func wireAddrs(db *sql.DB, c *Cycle) {
	for i := 0; i < 3; i++ {
		a := addr20(db, c.pools[i])
		copy(c.ap.Addr[i][:], a[:])
		c.ap.Dir[i] = (i == 1)
	}
	a := addr20(db, c.last)
	copy(c.ap.Addr[2][:], a[:])
	c.ap.Dir[2] = true
}

func buildSplit(memo map[[20]byte][2]*DeltaBucket, c *Cycle) {
	for leg := 0; leg < 3; leg++ {
		a := c.ap.Addr[leg]
		dir := c.ap.Dir[leg]
		entry := memo[a]
		idx := btoi(dir)
		if entry[idx] == nil {
			b := &DeltaBucket{Queue: bucketqueue.New()}
			entry[idx] = b
			memo[a] = entry
			core := idx*(len(routers)/2) + (int(a[0]) % (len(routers) / 2))
			routers[core].bucketByKey.Put(string(a[:]), uint16(len(routers[core].buckets)))
			routers[core].buckets = append(routers[core].buckets, b)
		}
	}

	ap := *c.ap
	q := memo[ap.Addr[0]][btoi(ap.Dir[0])].Queue
	h, _ := q.Borrow()
	*(*ArbPath)(unsafe.Pointer(&q.Arena[h])) = ap
	q.Push(0, h)

	for _, i := range [...]int{1, 2} {
		k := string(ap.Addr[i][:])
		dir := ap.Dir[i]
		core := btoi(dir)*(len(routers)/2) + (int(ap.Addr[i][0]) % (len(routers) / 2))
		router := routers[core]
		j, ok := router.queueByKey.Get(k)
		if !ok {
			j = uint16(len(router.fanOut))
			router.queueByKey.Put(k, j)
			router.fanOut = append(router.fanOut, nil)
		}
		router.fanOut[j] = append(router.fanOut[j], ref{Q: q, H: h})
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func fastAtoi(b []byte) int {
	n := 0
	for _, c := range b {
		n = n*10 + int(c-'0')
	}
	return n
}

func addr20(db *sql.DB, id int) (out [20]byte) {
	var hs string
	err := db.QueryRow("SELECT address FROM pools WHERE id=?", id).Scan(&hs)
	if err != nil {
		log.Fatalf("addr20: %v", err)
	}
	if _, err := hex.Decode(out[:], []byte(hs[2:])); err != nil {
		log.Fatalf("addr20 decode: %v", err)
	}
	return
}
