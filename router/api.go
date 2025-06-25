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

// RegisterCycles wires a batch of arbitrage cycles into all active CoreRouters.
// It registers pairs, assigns route masks, initializes per-core queues,
// creates shared ArbPath objects, and hooks up fan-out references.
func RegisterCycles(cycles []Cycle) {
	nCores := len(routers)
	mask := uint16((1 << nCores) - 1)

	for _, cyc := range cycles {
		// Register pair IDs with all-core route bitmap
		for _, pair := range cyc.Pairs {
			RegisterRoute(pair, mask)
		}

		// Shared path object for all legs
		path := &ArbPath{
			PoolID: [3]uint32{uint32(cyc.Pairs[0]), uint32(cyc.Pairs[1]), uint32(cyc.Pairs[2])},
			Dir:    [3]bool{false, false, false},
		}

		for i, pairID := range cyc.Pairs {
			for coreID := 0; coreID < nCores; coreID++ {
				rt := routers[coreID]
				idx := rt.PairToLocal[pairID]

				// Initialize per-core bucket if needed
				if idx == 0 && (len(rt.Buckets) == 0 || rt.Buckets[0] == nil || rt.Buckets[0].Queue == nil) {
					q := bucketqueue.New()
					rt.Buckets = append(rt.Buckets, &DeltaBucket{Queue: q})
					rt.PairToLocal[pairID] = uint32(len(rt.Buckets) - 1)
					idx = rt.PairToLocal[pairID]
				}

				// Attach fan-out reference
				q := rt.Buckets[idx].Queue
				h, _ := q.Borrow()
				_ = q.Push(2048, h, unsafe.Pointer(path))
				ref := fanRef{P: path, Q: q, H: h, SharedLeg: uint8(i)}

				for len(rt.FanOut) <= int(idx) {
					rt.FanOut = append(rt.FanOut, nil)
				}
				rt.FanOut[idx] = append(rt.FanOut[idx], ref)
			}
		}
	}
}
