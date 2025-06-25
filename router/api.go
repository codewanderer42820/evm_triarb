// router/fanout.go — highly‑efficient PathRef fan‑out builder
//
// Design goals:
//   - **Zero allocations per registration** beyond slice growth.
//   - **Single tight loop** over cycles — no intermediate structures.
//   - **Minimised map rehashing** by pre‑sizing `Forward`.
//   - `PathRef` embeds the full 3‑pair triangle and the exact index (`Pos`) of
//     the key leg, so downstream consumers can fetch the sibling legs with one
//     mod‑3 increment.
package router

import (
	"fmt"
	"sort"
)

// Cycle is one arbitrage triangle expressed as three pairIDs in canonical order.
// Tokens are reserved for future metadata but ignored by the fan‑out logic.
type Cycle struct {
	Tokens [3]uint32
	Pairs  [3]uint16 // e.g. [1,2,3]
}

// PathRef links a leg (Path[Pos]) back to its full triangle.
// Pos ∈ {0,1,2}.
type PathRef struct {
	Path [3]uint16
	Pos  uint8
}

// Forward maps pairID → all PathRefs that include that pair.
var Forward map[uint16][]PathRef

// ResetFanouts clears global state.  Call in tests or before rebuilding.
func ResetFanouts() { Forward = make(map[uint16][]PathRef) }

// BuildFanouts registers every cycle under each of its three legs.
//
// Complexity: O(N) where N = len(cycles).  No secondary passes.
func BuildFanouts(cycles []Cycle) {
	// One map entry per leg.  Pre‑size to avoid growth rehashing.
	required := len(cycles) * 3
	if Forward == nil {
		Forward = make(map[uint16][]PathRef, required)
	}

	for i := range cycles { // range by index to avoid copying Cycle struct
		path := cycles[i].Pairs // local copy of [3]uint16 (on stack)
		for pos, pair := range path {
			ref := PathRef{Path: path, Pos: uint8(pos)}
			Forward[pair] = append(Forward[pair], ref)
		}
	}
}

// DebugString prints the Forward map in ascending key order — handy in tests.
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
