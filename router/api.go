// router/api.go — clean router registration interface for cycles
package router

import (
	"main/bucketqueue"
	"unsafe"
)

// Cycle represents a closed arbitrage path: TokenA → (PairAB) → TokenB → (PairBC) → TokenC → (PairCA) → TokenA
// All fields refer to internal DB IDs (not raw addresses).
type Cycle struct {
	Tokens [3]uint32 // token IDs
	Pairs  [3]uint16 // pair IDs
}

// RegisterCycles wires a batch of arbitrage cycles into every CoreRouter.
//
// Invariant:
//   - rt.PairIndex[pairID] == 0  →  pair is not yet wired on this core
//   - rt.PairIndex[pairID] >  0  →  Routes[idx].Queue is non-nil
func RegisterCycles(cycles []Cycle) {
	nCores := len(coreRouters)
	mask := uint16((1 << nCores) - 1) // CPU-bitmap for every pair

	for _, cyc := range cycles {
		// 1) expose all three pairs to the producer side
		for _, pair := range cyc.Pairs {
			RegisterRoute(pair, mask)
		}

		// 2) shared path reused by every core / leg
		path := &ArbPath{
			PoolID:  [3]uint32{uint32(cyc.Pairs[0]), uint32(cyc.Pairs[1]), uint32(cyc.Pairs[2])},
			Reverse: [3]bool{},
		}

		// 3) per-core wiring
		for _, rt := range coreRouters {
			for leg, pairID := range cyc.Pairs {
				idx := rt.PairIndex[pairID]

				// ── allocate a bucket once per (core, pair) ───────────────
				if idx == 0 { // unmapped sentinel
					q := bucketqueue.New()
					rt.Routes = append(rt.Routes, &DeltaBucket{Queue: q})
					idx = uint32(len(rt.Routes)) // store index+1
					rt.PairIndex[pairID] = idx
				}
				bktIdx := idx - 1 // restore real index
				q := rt.Routes[bktIdx].Queue

				// seed queue so PeepMin() is non-nil in unit-tests
				h, _ := q.Borrow()
				_ = q.Push(0, h, unsafe.Pointer(path))

				// ensure Fanouts slice is long enough
				for len(rt.Fanouts) <= int(bktIdx) {
					rt.Fanouts = append(rt.Fanouts, nil)
				}
				rt.Fanouts[bktIdx] = append(rt.Fanouts[bktIdx], fanRef{
					P:         path,
					Q:         q,
					H:         h,
					SharedLeg: uint8(leg),
				})
			}
		}
	}
}
