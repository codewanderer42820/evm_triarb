package router

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

const (
	Fwd = 0
	Rev = 1
)

var keyCache = make(map[[21]byte]string)

func buildKeys(addr [20]byte) {
	for _, dir := range []int{Fwd, Rev} {
		var key [21]byte
		copy(key[:20], addr[:])
		key[20] = byte(dir)
		keyCache[key] = string(key[:])
	}
}

func keyOf(addr *[20]byte, dir int) string {
	var key [21]byte
	copy(key[:20], addr[:])
	key[20] = byte(dir)
	if v, ok := keyCache[key]; ok {
		return v
	}
	return string(key[:]) // fallback if not cached
}

func build(workers int, memo map[[21]byte]*DeltaBucket, rtr []*CoreRouter, p struct {
	pools [3]int
	last  int
	ap    *ArbPath
}) {
	// Fill dummy ap if nil
	if p.ap == nil {
		p.ap = &ArbPath{}
		for i := 0; i < 3; i++ {
			p.ap.Addr[i][19] = byte(p.pools[i])
			p.ap.Dir[i] = (i == 1 || i == 2)
		}
	}

	// Build 3 buckets per address+dir combo
	for i := 0; i < 3; i++ {
		key := p.ap.Addr[i]
		dir := p.ap.Dir[i]
		idx := btoi(dir)
		k := [21]byte{}
		copy(k[:20], key[:])
		k[20] = byte(idx)

		if memo[k] == nil {
			memo[k] = &DeltaBucket{Queue: bucketqueue.New()}
			core := 0
			r := rtr[core]
			r.bucketByKey.Put(string(key[:]), uint16(len(r.buckets)))
			r.buckets = append(r.buckets, memo[k])
		}
	}

	// Push ArbPath into Arena, update queue
	ap := *p.ap
	k := [21]byte{}
	copy(k[:20], ap.Addr[0][:])
	k[20] = byte(btoi(ap.Dir[0]))
	q := memo[k].Queue
	h, _ := q.Borrow()
	*(*ArbPath)(unsafe.Pointer(&q.Arena[h])) = ap
	q.Push(0, h)

	// Set fanOut for other legs
	for _, i := range []int{1, 2} {
		r := rtr[0]
		j, ok := r.queueByKey.Get(string(ap.Addr[i][:]))
		if !ok {
			j = uint16(len(r.fanOut))
			r.queueByKey.Put(string(ap.Addr[i][:]), j)
			r.fanOut = append(r.fanOut, nil)
		}
		r.fanOut[j] = append(r.fanOut[j], ref{Q: q, H: h})
	}
}

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

	for sc.Scan() {
		line := sc.Bytes()
		var pools [3]int
		idx := 0
		num := 0
		inParens := false

		for _, b := range line {
			switch {
			case b == '(':
				inParens = true
				num = 0
			case b == ')' && inParens:
				if idx < 3 {
					pools[idx] = num
					idx++
				}
				inParens = false
			case inParens && b >= '0' && b <= '9':
				num = num*10 + int(b-'0')
			}
		}

		if idx == 3 {
			out = append(out, Cycle{
				pools: pools,
				last:  pools[2], // dummy fallback if you want
				ap:    &ArbPath{},
			})
		}
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

func parseLine(line []byte) [3]int {
	var out [3]int
	n := 0
	insideParens := false
	num := 0
	capturing := false

	for _, b := range line {
		switch b {
		case '(':
			insideParens = true
			num = 0
			capturing = false
		case ')':
			if insideParens && capturing && n < len(out) {
				out[n] = num
				n++
			}
			insideParens = false
		default:
			if insideParens && b >= '0' && b <= '9' {
				num = num*10 + int(b-'0')
				capturing = true
			}
		}
	}

	return out
}
