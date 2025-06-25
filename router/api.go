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
	nCores := len(coreRouters)
	mask := uint16((1 << nCores) - 1)

	for _, cyc := range cycles {
		// Register pair IDs with all-core route bitmap
		for _, pair := range cyc.Pairs {
			RegisterRoute(pair, mask)
		}

		// Shared path object for all legs
		path := &ArbPath{
			PoolID:  [3]uint32{uint32(cyc.Pairs[0]), uint32(cyc.Pairs[1]), uint32(cyc.Pairs[2])},
			Reverse: [3]bool{false, false, false},
		}

		for i, pairID := range cyc.Pairs {
			for coreID := 0; coreID < nCores; coreID++ {
				rt := coreRouters[coreID]
				idx := rt.PairIndex[pairID]

				// Initialize per-core bucket if needed
				if idx == 0 && (len(rt.Routes) == 0 || rt.Routes[0] == nil || rt.Routes[0].Queue == nil) {
					q := bucketqueue.New()
					rt.Routes = append(rt.Routes, &DeltaBucket{Queue: q})
					rt.PairIndex[pairID] = uint32(len(rt.Routes) - 1)
					idx = rt.PairIndex[pairID]
				}

				// Attach fan-out reference
				q := rt.Routes[idx].Queue
				h, _ := q.Borrow()
				_ = q.Push(2048, h, unsafe.Pointer(path))
				ref := fanRef{P: path, Q: q, H: h, SharedLeg: uint8(i)}

				for len(rt.Fanouts) <= int(idx) {
					rt.Fanouts = append(rt.Fanouts, nil)
				}
				rt.Fanouts[idx] = append(rt.Fanouts[idx], ref)
			}
		}
	}
}
