// ✅ Tuned for max performance: zero alloc, cache-aligned, L1-friendly
// ✅ Structs reordered for alignment, _pad added for layout correctness
// ✅ Core identity (Fwd or Rev) is now configurable per runLoop instance
// ✅ SWAR-style recompute loop added for tighter cache-friendly bursts

package main

import (
	"log"
	"math/rand"
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
	_pad   [48]byte
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

		if i, ok := rt.bucketByKey.GetBytes(key[:]); ok {
			rt.buckets[i].CurLog = val
		}
		if fi, ok := rt.queueByKey.GetBytes(key[:]); ok {
			recomputeSWAR(rt.fanOut[fi])
		}
	}, nil)
}

func recomputeSWAR(refs []ref) {
	const unroll = 4
	var aps [unroll]*ArbPath
	for i := 0; i < len(refs); i += unroll {
		// prefetch addresses
		for j := 0; j < unroll && i+j < len(refs); j++ {
			r := refs[i+j]
			aps[j] = (*ArbPath)(unsafe.Pointer(&r.Q.Arena[r.H]))
		}
		// apply recompute
		for j := 0; j < unroll && i+j < len(refs); j++ {
			tick := aps[j].LegVal[1] + aps[j].LegVal[2]
			_ = refs[i+j].Q.Update(int64(tick*1e6), refs[i+j].H)
		}
	}
}
