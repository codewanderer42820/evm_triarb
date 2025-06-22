// Package router implements a zero-alloc, O(1), branch-stable routing graph constructor.
package router

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"os"
	"runtime"
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

var (
	routers []*CoreRouter
	rings   []*ring.Ring
)

//lint:ignore U1000 used for cache-alignment
var _pad [64]byte

// SWAR-style constants
const swarMask = 0x0f0f0f0f0f0f0f0f

// Avoid bounds-check on fixed count
const maxParens = 3

// Structs below are pre-aligned and direct-access

type DeltaBucket struct {
	Queue  *bucketqueue.Queue
	CurLog float64
	_pad   [48]byte // 64B alignment
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
		panic("need ≥ 2 CPUs")
	}

	routers = make([]*CoreRouter, n)
	rings = make([]*ring.Ring, n)
	for i := 0; i < n; i++ {
		routers[i] = &CoreRouter{
			bucketByKey: pairidx.New(),
			queueByKey:  pairidx.New(),
		}
		rings[i] = ring.New(1 << 14)
		go runLoop(i, rings[i], routers[i], i >= n/2)
	}

	db := mustDB("uniswap_pairs.db")
	pairs := parseCycles("cycles_3_3.txt")
	bucketTable := make(map[[20]byte][2]*DeltaBucket, 4096)

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
		key := *(*[20]byte)(unsafe.Pointer(&u.Addr[0]))
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
	db, _ := sql.Open("sqlite3", path)
	_ = db.Ping()
	return db
}

func parseCycles(path string) []Cycle {
	f, _ := os.Open(path)
	sc := bufio.NewScanner(f)
	var out []Cycle
	var pools [3]int
	for sc.Scan() {
		line := sc.Bytes()
		n := 0
		in, val := false, 0
		for i := 0; i < len(line); i++ {
			b := line[i]
			if b == '(' {
				val, in = 0, true
				continue
			}
			if b == ')' && in {
				if n == 0 {
					pools[0] = val
				} else if n == 1 {
					pools[1] = val
				} else if n == 2 {
					pools[2] = val
				}
				n++
				in = false
				continue
			}
			if in && b >= '0' && b <= '9' {
				val = val*10 + int(b-'0')
			}
		}
		if n == 3 {
			out = append(out, Cycle{pools: pools, last: pools[2], ap: &ArbPath{}})
		}
	}
	return out
}

func wireAddrs(db *sql.DB, c *Cycle) {
	for i := 0; i < 3; i++ {
		addr := addr20(db, c.pools[i])
		c.ap.Addr[i] = addr
		c.ap.Dir[i] = (i == 1)
	}
	c.ap.Addr[2] = addr20(db, c.last)
	c.ap.Dir[2] = true
}

func buildSplit(memo map[[20]byte][2]*DeltaBucket, c *Cycle) {
	for i := 0; i < 3; i++ {
		a := c.ap.Addr[i]
		d := btoi(c.ap.Dir[i])
		e := memo[a]
		if e[d] == nil {
			b := &DeltaBucket{Queue: bucketqueue.New()}
			e[d] = b
			memo[a] = e
			core := d*(len(routers)/2) + int(a[0])%(len(routers)/2)
			r := routers[core]
			r.bucketByKey.Put(string(a[:]), uint16(len(r.buckets)))
			r.buckets = append(r.buckets, b)
		}
	}
	ap := *c.ap
	q := memo[ap.Addr[0]][btoi(ap.Dir[0])].Queue
	h, _ := q.Borrow()
	*(*ArbPath)(unsafe.Pointer(&q.Arena[h])) = ap
	q.Push(0, h)
	for _, i := range [...]int{1, 2} {
		k := string(ap.Addr[i][:])
		d := ap.Dir[i]
		core := btoi(d)*(len(routers)/2) + int(ap.Addr[i][0])%(len(routers)/2)
		r := routers[core]
		j, ok := r.queueByKey.Get(k)
		if !ok {
			j = uint16(len(r.fanOut))
			r.queueByKey.Put(k, j)
			r.fanOut = append(r.fanOut, nil)
		}
		r.fanOut[j] = append(r.fanOut[j], ref{Q: q, H: h})
	}
}

func addr20(db *sql.DB, id int) (out [20]byte) {
	var hs string
	_ = db.QueryRow("SELECT address FROM pools WHERE id=?", id).Scan(&hs)
	_, _ = hex.Decode(out[:], []byte(hs[2:]))
	return
}

func btoi(b bool) int {
	return int(*(*uint8)(unsafe.Pointer(&b)))
}
