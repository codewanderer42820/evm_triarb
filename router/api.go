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
//
// All IDs here are internal DB IDs, not raw addresses.
type Cycle struct {
	Tokens [3]uint32 // three unique token IDs
	Pairs  [3]uint16 // three corresponding pair IDs
}

// RegisterCycles randomly enables exactly two cores per cycle:
// one chosen from the “forward” half (indices [0..half-1]),
// one chosen from the “reverse” half ([half..nCores-1]).
// We OR those two single-bit masks together so RegisterRoute only
// flips those two bits on.
func RegisterCycles(cycles []Cycle) {
	nCores := len(coreRouters)
	if nCores == 0 {
		return
	}
	half := nCores / 2

	for _, cyc := range cycles {
		// pick one forward-core index [0,half)
		fwdCore := rand.Intn(half)
		// pick one reverse-core index [half, nCores)
		revCore := rand.Intn(nCores-half) + half

		// build a mask with just those two bits
		mask := uint16(1<<fwdCore | 1<<revCore)

		// register each pair under exactly those two cores
		for _, pairID := range cyc.Pairs {
			RegisterRoute(pairID, mask)
		}

		// wire up only those two cores
		for _, coreIdx := range []int{fwdCore, revCore} {
			rt := coreRouters[coreIdx]
			legPairs := cyc.Pairs

			// build one shared ArbPath for this core
			// Reverse flags nominally unused here, since onPriceUpdate
			// uses CoreRouter.IsReverse to pick RevTick vs FwdTick.
			path := &ArbPath{
				PoolID: [3]uint32{
					uint32(legPairs[0]),
					uint32(legPairs[1]),
					uint32(legPairs[2]),
				},
			}

			// for each leg, ensure a queue exists and seed it
			for legIdx, pairID := range legPairs {
				idx := rt.PairIndex[pairID]
				if idx == 0 {
					// first time for this pair on this core; allocate
					q := bucketqueue.New()
					rt.Routes = append(rt.Routes, &DeltaBucket{Queue: q})
					idx = uint32(len(rt.Routes))
					rt.PairIndex[pairID] = idx
				}

				bktIndex := idx - 1
				q := rt.Routes[bktIndex].Queue

				// seed so PeepMin never returns nil
				h, _ := q.Borrow()
				_ = q.Push(0, h, unsafe.Pointer(path))

				// make sure the fanout slot exists
				if len(rt.Fanouts) <= int(bktIndex) {
					needed := int(bktIndex) - len(rt.Fanouts) + 1
					rt.Fanouts = append(rt.Fanouts, make([][]fanRef, needed)...)
				}
				rt.Fanouts[bktIndex] = append(rt.Fanouts[bktIndex], fanRef{
					P:         path,
					Q:         q,
					H:         h,
					SharedLeg: uint8(legIdx),
				})
			}
		}
	}
}
