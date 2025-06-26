package router

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"runtime"
	"runtime/debug"
	"sync"
	"unsafe"

	"main/fastuni"
	"main/localidx"
	"main/ring"
	"main/types"
	"main/utils"
)

// ---------- domain ----------

type PairID uint32
type LocalPairID uint32
type CPUMask uint64
type TriCycle [3]PairID

// ArbPath holds the ticks and pairs for arbitrage paths
type ArbPath struct {
	Ticks [3]float64
	Pairs [3]PairID
}

// Ref represents a reference in an arbitrage path
type Ref struct {
	Pairs TriCycle
	Edge  uint16
	_pad  [3]byte
}

// Shard represents a pair of Fanouts
type Shard struct {
	Pair PairID
	Refs []Ref
}

// Fanouts is a map of LocalPairID to a slice of Fanouts
type Fanouts map[LocalPairID][]Fanout

var (
	fanouts        Fanouts
	splitThreshold = 16_384
)

// ---------- crypto shuffle ----------

func crandInt(n int) int {
	var b [8]byte
	rand.Read(b[:])
	v := binary.LittleEndian.Uint64(b[:])
	if n&(n-1) == 0 {
		return int(v & uint64(n-1))
	}
	hi, _ := bits.Mul64(v, uint64(n))
	return int(hi)
}

func shuffleRefs(r []Ref) {
	for i := len(r) - 1; i > 0; i-- {
		j := crandInt(i + 1)
		r[i], r[j] = r[j], r[i]
	}
}

// ---------- BuildFanouts ----------

func ResetFanouts() { fanouts = make(Fanouts) }

func BuildFanouts(cycles []TriCycle) {
	debug.SetGCPercent(100)
	defer debug.SetGCPercent(-1)

	ResetFanouts()
	tmp := make(map[PairID][]Ref, len(cycles)*3)
	for _, tri := range cycles {
		for pos, pair := range tri {
			tmp[pair] = append(tmp[pair], Ref{Pairs: tri, Edge: uint16(pos)})
		}
	}
	for pair, refs := range tmp {
		shuffleRefs(refs)
		for off := 0; off < len(refs); off += splitThreshold {
			end := off + splitThreshold
			if end > len(refs) {
				end = len(refs)
			}
			fanouts[pair] = append(fanouts[pair], Shard{Pair: pair, Refs: refs[off:end]})
		}
	}
	runtime.GC()
}

// ---------- mapper ----------

const (
	clampL2  = 64
	buckets  = 4096
	scaleMul = buckets - 1
	scaleDiv = clampL2 * 2
	zeroOff  = buckets / 2
)

func mapL2ToBucket(l2 float64) int64 {
	if l2 > clampL2 {
		l2 = clampL2
	} else if l2 < -clampL2 {
		l2 = -clampL2
	}
	return int64(zeroOff) + int64(l2*scaleMul/scaleDiv)
}

// ---------- Hot structs ----------

type TickBucket = tickSoA // alias

// ---------- core router ----------

type CoreRouter struct {
	Buckets []TickBucket
	Fanouts Fanouts
	Local   localidx.Hash
	IsRev   bool
	ShardCh chan Shard
}

type PriceUpdate struct {
	Pair             PairID
	FwdTick, RevTick float64
}

// ---------- globals ----------

var (
	coreRings    [64]*ring.Ring
	coreRouters  []*CoreRouter
	routeList    [1 << 17][]uint8
	addrToPairID [1 << 17]PairID
)

// ---------- init ----------

// Convert global PairID to LocalPairID
func GlobalPairIDToLocalPairID(rt *CoreRouter, pid PairID) LocalPairID {
	return LocalPairID(pid)
}

func InitCPURings(cycles []TriCycle) {
	n := runtime.NumCPU() - 4
	if n < 8 {
		n = 8
	}
	if n > 64 {
		n = 64
	}
	BuildFanouts(cycles)

	coreRouters = make([]*CoreRouter, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for core := 0; core < n; core++ {
		go func(core int) {
			defer wg.Done()
			runtime.LockOSThread()

			rb := ring.New(15873)
			shCh := make(chan Shard, 256)
			rt := &CoreRouter{
				IsRev:   core >= n/2,
				Buckets: make([]TickBucket, 0, 1024),
				Fanouts: make(Fanouts),
				Local:   localidx.New(1 << 16),
				ShardCh: shCh,
			}
			coreRouters[core] = rt
			coreRings[core] = rb

			for sh := range shCh {
				installShard(rt, &sh)
			}

			ring.PinnedConsumer(core, rb, new(uint32), new(uint32),
				func(p unsafe.Pointer) { onPrice(rt, (*PriceUpdate)(p)) },
				make(chan struct{}))
		}(core)
	}

	go func() {
		core := 0
		for _, shards := range fanouts {
			for _, sh := range shards {
				coreRouters[core%n].ShardCh <- sh
				core++
			}
		}
		for _, rt := range coreRouters {
			close(rt.ShardCh)
		}
	}()
	wg.Wait()
}

// Retrieve the LocalPairID from the hash map
func LocalPairKey(rt *CoreRouter, pid PairID) LocalPairID {
	// Convert global PairID to LocalPairID using the conversion function
	return GlobalPairIDToLocalPairID(rt, pid)
}

// Install a shard of data into the core router
func installShard(rt *CoreRouter, sh *Shard) {
	lid := LocalPairKey(rt, sh.Pair) // Correctly using LocalPairKey here
	b := &rt.Buckets[lid]
	b.ensureCap(len(sh.Refs))

	if rt.Fanouts[lid] == nil {
		rt.Fanouts[lid] = []Fanout{}
	}

	for i, ref := range sh.Refs {
		rt.Fanouts[lid] = append(rt.Fanouts[lid], Fanout{
			Pairs: ref.Pairs,
			Queue: &b.Queue,
			Edge:  ref.Edge,
			Idx:   uint16(i),
		})
	}
}

// Process price updates based on the current state
func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsRev {
		tick = upd.RevTick
	}
	lid, ok := rt.Local.Get(uint32(upd.Pair))
	if !ok {
		return
	}

	b := &rt.Buckets[lid]
	for _, f := range rt.Fanouts[lid] {
		switch f.Edge {
		case 0:
			b.t0[f.Idx] = tick
		case 1:
			b.t1[f.Idx] = tick
		default:
			b.t2[f.Idx] = tick
		}

		sum := b.t0[f.Idx] + b.t1[f.Idx] + b.t2[f.Idx]
		f.Queue.Update(mapL2ToBucket(sum), 0, unsafe.Pointer(&f.Pairs))
	}
}

// Route the log update to the relevant price processors
func RouteUpdate(v *types.LogView) {
	addr := v.Addr[3:43]
	pair := lookupPairID(addr)

	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	fwd := fastuni.Log2ReserveRatio(r0, r1)

	upd := PriceUpdate{Pair: pair, FwdTick: fwd, RevTick: -fwd}
	ptr := unsafe.Pointer(&upd)
	for _, c := range routeList[pair] {
		coreRings[c].Push(ptr)
	}
}

// Register a pair in the address-to-pair mapping
func RegisterPair(addr40 []byte, pid PairID) {
	idx := utils.Hash17(addr40)
	for {
		if addrToPairID[idx] == 0 {
			addrToPairID[idx] = pid
			return
		}
		idx = (idx + 64) & ((1 << 17) - 1)
	}
}

// Register a route for a specific pair
func RegisterRoute(pid PairID, coreID uint8) {
	routeList[pid] = append(routeList[pid], coreID)
}

// Lookup a pair ID based on the address
func lookupPairID(addr []byte) PairID {
	idx := utils.Hash17(addr)
	for {
		if id := addrToPairID[idx]; id != 0 {
			return id
		}
		idx = (idx + 64) & ((1 << 17) - 1)
	}
}

func onProfitablePath(_ *ArbPath, _ float64) {} // Stub
