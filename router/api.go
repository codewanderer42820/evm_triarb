// router/api.go — clean router registration interface for cycles
package router

import (
	"main/bucketqueue"
	"math/rand"
	"time"
	"unsafe"
)

// seed the RNG once
func init() {
	rand.Seed(time.Now().UnixNano())
}

// Cycle represents a closed arbitrage path:
//
//	TokenA → (PairAB) → TokenB → (PairBC) → TokenC → (PairCA) → TokenA
type Cycle struct {
	Tokens [3]uint32 // three unique token IDs
	Pairs  [3]uint16 // three corresponding pair IDs
}

// coreMask builds a 64-bit mask for two cores.
func coreMask(fwd, rev int) uint64 {
	return (uint64(1) << fwd) | (uint64(1) << rev)
}

// RegisterCycles enables exactly two cores per pair in each cycle,
// selecting 6 distinct cores per cycle (2 for each of 3 pairs).
func RegisterCycles(cycles []Cycle) {
	nCores := len(coreRouters)
	if nCores < 6 {
		return // not enough cores for full dispersion
	}

	for _, cyc := range cycles {
		// pick 6 distinct cores via a single permutation
		perm := rand.Perm(nCores)
		coreSet := perm[:6]

		// assign each pair (fwd, rev) and register route
		for i, pairID := range cyc.Pairs {
			fwd := coreSet[i*2]
			rev := coreSet[i*2+1]
			RegisterRoute(pairID, coreMask(fwd, rev))
		}

		// allocate per-core path data and seed queues
		for _, coreIdx := range coreSet {
			rt := coreRouters[coreIdx]
			path := &ArbPath{PoolID: [3]uint32{
				uint32(cyc.Pairs[0]),
				uint32(cyc.Pairs[1]),
				uint32(cyc.Pairs[2]),
			}}

			for legIdx, pairID := range cyc.Pairs {
				idx := rt.PairIndex[pairID]
				if idx == 0 {
					q := bucketqueue.New()
					rt.Routes = append(rt.Routes, &DeltaBucket{Queue: q})
					idx = uint32(len(rt.Routes))
					rt.PairIndex[pairID] = idx
				}
				bkt := idx - 1
				q := rt.Routes[bkt].Queue
				h, _ := q.Borrow()
				_ = q.Push(0, h, unsafe.Pointer(path))

				// fanout slice was preallocated in InitCPURings
				rt.Fanouts[bkt] = append(rt.Fanouts[bkt], fanRef{
					P:         path,
					Q:         q,
					H:         h,
					SharedLeg: uint8(legIdx),
				})
			}
		}
	}
}
