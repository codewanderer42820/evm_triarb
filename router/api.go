// router/fanout.go — fan‑out map builder using PathRef
//
// Each arbitrage cycle is stored once per leg in a map keyed by the leg’s
// pairID.  A PathRef keeps the full triangle plus the index of the key pair so
// downstream code can jump directly to the two sibling legs without extra
// scans or allocations.
package router

import (
	"fmt"
	"sort"
)

// Cycle represents a 3‑pair arbitrage triangle.
type Cycle struct {
	Tokens [3]uint32 // reserved for future metadata
	Pairs  [3]uint16 // [PairAB, PairBC, PairCA]
}

// PathRef links one leg (Path[Pos]) back to the full triangle.
// Pos is always 0, 1, or 2.
type PathRef struct {
	Path [3]uint16
	Pos  uint8
}

// Forward maps pairID → all PathRefs that include that pair.
var Forward map[uint16][]PathRef

// ResetFanouts clears global state (use in tests).
func ResetFanouts() { Forward = make(map[uint16][]PathRef) }

// BuildFanouts registers every cycle under each of its three legs.
func BuildFanouts(cycles []Cycle) {
	if Forward == nil {
		ResetFanouts()
	}
	for _, cyc := range cycles {
		for i, pair := range cyc.Pairs {
			Forward[pair] = append(Forward[pair], PathRef{Path: cyc.Pairs, Pos: uint8(i)})
		}
	}
}

// DebugString pretty‑prints the Forward map (handy while prototyping).
func DebugString() string {
	keys := make([]int, 0, len(Forward))
	for k := range Forward {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	out := ""
	for _, k := range keys {
		out += fmt.Sprintf("%d: %+v\n", k, Forward[uint16(k)])
	}
	return out
}
