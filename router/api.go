// router/fanout.go — minimal‑footprint fan‑out with **cryptographically secure** randomness
//
// Changes in this revision
// ------------------------
//   - Replaced all uses of math/rand with a helper that falls back to
//     **crypto/rand** (CSPRNG). If EmitCfg.Seed != 0 we keep a deterministic
//     math/rand path for reproducible tests; otherwise we source every random
//     index from crypto/rand.
//   - secureShuffle implements Fisher‑Yates with CSPRNG.
//   - `emitLoop` uses the same helper for random pop.
package router

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	mathrand "math/rand" // alias to differentiate from crypto/rand
	"runtime"
	"sort"
)

// -----------------------------------------------------------------------------
// Triangle table (deduplicated)
// -----------------------------------------------------------------------------

var triTable [][3]uint32
var triIndex map[[3]uint32]uint32

func resetTriTable(capHint int) {
	triTable = make([][3]uint32, 0, capHint)
	triIndex = make(map[[3]uint32]uint32, capHint)
}

func triID(t [3]uint32) uint32 {
	if id, ok := triIndex[t]; ok {
		return id
	}
	id := uint32(len(triTable))
	triTable = append(triTable, t)
	triIndex[t] = id
	return id
}

func GetTri(id uint32) [3]uint32 { return triTable[id] }

// -----------------------------------------------------------------------------
// Core types (compact)
// -----------------------------------------------------------------------------

type Cycle struct{ Pairs [3]uint32 }

type PathRef struct {
	Tri uint32
	Pos uint8
}

type ShardItem struct {
	Pair uint32
	Refs []PathRef // read‑only if Move=true
}

// -----------------------------------------------------------------------------
// Cryptographically‑secure RNG helpers
// -----------------------------------------------------------------------------

// crandInt returns a uniform random int in [0,n) using crypto/rand.
func crandInt(n int) int {
	for {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			panic(err) // crypto/rand should never fail on *nix
		}
		v := binary.LittleEndian.Uint64(b[:])
		if vMax := uint64(n); vMax&(vMax-1) == 0 {
			// power‑of‑two fast path
			return int(v & (vMax - 1))
		} else {
			if x := v % vMax; v-vMax+x < vMax { // unbiased discard trick
				return int(x)
			}
		}
	}
}

// secureInt chooses between deterministic math/rand (if rng!=nil) or crypto.
func secureInt(n int, rng *mathrand.Rand) int {
	if rng != nil {
		return rng.Intn(n)
	}
	return crandInt(n)
}

func secureShuffle(refs []PathRef, rng *mathrand.Rand) {
	for i := len(refs) - 1; i > 0; i-- {
		j := secureInt(i+1, rng)
		refs[i], refs[j] = refs[j], refs[i]
	}
}

// -----------------------------------------------------------------------------
// Staging map + builder
// -----------------------------------------------------------------------------

var Forward map[uint32][][]PathRef

const splitThreshold = 16_384

func ResetFanouts() {
	Forward = make(map[uint32][][]PathRef)
	resetTriTable(1024)
}

func BuildFanouts(cycles []Cycle) {
	ResetFanouts()

	tmp := make(map[uint32][]PathRef, len(cycles)*3)

	for i := range cycles {
		tri := cycles[i].Pairs
		id := triID(tri)
		for pos, pair := range tri {
			tmp[pair] = append(tmp[pair], PathRef{Tri: id, Pos: uint8(pos)})
		}
	}

	usable := runtime.NumCPU() - 4
	if usable < 1 {
		usable = 1
	}

	Forward = make(map[uint32][][]PathRef, len(tmp))

	for pair, refs := range tmp {
		if len(refs) <= splitThreshold || usable == 1 {
			Forward[pair] = [][]PathRef{refs}
			continue
		}
		secureShuffle(refs, nil)
		shards := make([][]PathRef, usable)
		for idx, ref := range refs {
			shards[idx%usable] = append(shards[idx%usable], ref)
		}
		Forward[pair] = shards
	}
	triIndex = nil
}

// -----------------------------------------------------------------------------
// Emission APIs
// -----------------------------------------------------------------------------

type EmitCfg struct {
	Buffer int
	Seed   int64 // if non‑zero => deterministic math/rand, else crypto/rand
	Move   bool
}

func EmitShards(cfg EmitCfg) <-chan ShardItem {
	if cfg.Buffer == 0 {
		cfg.Buffer = 1024
	}
	ch := make(chan ShardItem, cfg.Buffer)
	go emitLoop(cfg, ch, nil)
	return ch
}

func EmitShardsPerCore(cfg EmitCfg) []<-chan ShardItem {
	usable := runtime.NumCPU() - 4
	if usable < 1 {
		usable = 1
	}
	if cfg.Buffer == 0 {
		cfg.Buffer = 256
	}
	chans := make([]chan ShardItem, usable)
	for i := range chans {
		chans[i] = make(chan ShardItem, cfg.Buffer)
	}
	go emitLoop(cfg, nil, chans)
	outs := make([]<-chan ShardItem, usable)
	for i := range chans {
		outs[i] = chans[i]
	}
	return outs
}

func emitLoop(cfg EmitCfg, single chan<- ShardItem, perCore []chan ShardItem) {
	defer func() {
		if single != nil {
			close(single)
		}
		for _, c := range perCore {
			if c != nil {
				close(c)
			}
		}
	}()
	if Forward == nil {
		return
	}

	var rng *mathrand.Rand
	if cfg.Seed != 0 {
		rng = mathrand.New(mathrand.NewSource(cfg.Seed))
	}

	type tuple struct {
		pair  uint32
		shard []PathRef
	}
	items := make([]tuple, 0)
	for pair, shards := range Forward {
		for _, s := range shards {
			items = append(items, tuple{pair, s})
		}
	}

	core := 0
	for len(items) > 0 {
		idx := secureInt(len(items), rng)
		it := items[idx]
		items[idx] = items[len(items)-1]
		items = items[:len(items)-1]

		var refs []PathRef
		if cfg.Move {
			refs = it.shard
		} else {
			refs = append([]PathRef(nil), it.shard...)
		}

		if single != nil {
			single <- ShardItem{Pair: it.pair, Refs: refs}
		} else {
			perCore[core] <- ShardItem{Pair: it.pair, Refs: refs}
			core = (core + 1) % len(perCore)
		}
	}
}

func Release() {
	Forward = nil
	triTable = nil
}

// -----------------------------------------------------------------------------
// Debug helper
// -----------------------------------------------------------------------------

func DebugString() string {
	keys := make([]uint32, 0, len(Forward))
	for k := range Forward {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := ""
	for _, k := range keys {
		out += fmt.Sprintf("%d: %v\n", k, Forward[k])
	}
	return out
}
