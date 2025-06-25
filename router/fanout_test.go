// router/fanout_test.go — tests for PathRef fan‑out builder
package router

import (
	"sort"
	"testing"
)

func buildCycles() []Cycle {
	return []Cycle{
		{Pairs: [3]uint16{1, 2, 3}},
		{Pairs: [3]uint16{2, 3, 4}},
		{Pairs: [3]uint16{3, 4, 5}},
		{Pairs: [3]uint16{1, 2, 5}},
		{Pairs: [3]uint16{2, 3, 5}},
		{Pairs: [3]uint16{5, 3, 1}},
	}
}

// TestPathRefInvariants: Path[Pos] must equal the map key.
func TestPathRefInvariants(t *testing.T) {
	ResetFanouts()
	BuildFanouts(buildCycles())

	for pair, refs := range Forward {
		for _, r := range refs {
			if r.Path[r.Pos] != pair {
				t.Fatalf("pair %d Path[%d]=%d mismatch in %v", pair, r.Pos, r.Path[r.Pos], r)
			}
		}
	}
}

// TestFanoutPrint dumps the Forward map for manual inspection.
// Run with: go test -v -run FanoutPrint ./router
func TestFanoutPrint(t *testing.T) {
	ResetFanouts()
	BuildFanouts(buildCycles())

	keys := make([]int, 0, len(Forward))
	for k := range Forward {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for _, k := range keys {
		t.Logf("%d: %+v", k, Forward[uint16(k)])
	}
}
