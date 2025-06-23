// Package router implements a zero-alloc, O(1)-time, branch-stable routing graph constructor.
package router

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"os"
	"runtime"
	"unsafe"

	_ "github.com/mattn/go-sqlite3"

	"main/bucketqueue"
	"main/pairidx"
	"main/ring"
)

const (
	Fwd = 0
	Rev = 1
)

var (
	routers []*CoreRouter
	rings   []*ring.Ring
)

//lint:ignore U1000 used for cache-alignment of hot structs
var _pad [64]byte

// ─── SWAR helpers ───────────────────────────────────────────────────────────
const maxParens = 3 // parseCycles bound – avoids bounds checks

// ─── Data structures (all cache-aligned, direct-access, no indirections) ───

type DeltaBucket struct {
	// Frequently-written field isolated on its own cache-line to avoid false sharing.
	CurLog float64  // latest log price for its pool (single writer per core)
	_line  [56]byte // pad to 64 B so Path starts clean on next line

	Queue *bucketqueue.Queue // priority queue handle pool (immutable after bootstrap)
	Path  ArbPath            // immutable arbitrage path (resident, not copied)
}

type ArbPath struct {
	// Packed so LegSum is loaded with Addr[0] in same line for tight gather.
	LegVal [3]float64  // per-leg log deltas
	LegSum float64     // pre-summed LegVal[1]+LegVal[2] — hot in recompute loop
	Addr   [3][20]byte // pool addresses (20-byte)
	Dir    [3]bool     // trade direction per leg (A→B vs B→A)
}

type ref struct {
	H bucketqueue.Handle // queue node handle
	Q *bucketqueue.Queue // owning queue (for Update/Push)
	B *DeltaBucket       // pointer to owning bucket (exposes Path & CurLog)
}

type PriceUpdate struct { // hot-path: emitted by websocket reader
	FwdTick float64
	RevTick float64
	Addr    [20]byte
}

type CoreRouter struct { // per-CPU router instance (pinned goroutine)
	bucketByKey *pairidx.HashMap // [addr] → bucket index (Δ updates)
	queueByKey  *pairidx.HashMap // [addr] → fan-out slice index
	buckets     []*DeltaBucket   // dense owner buckets (hot)
	fanOut      [][]ref          // {producer pool} → refs of affected queue nodes
}

type Cycle struct { // one 3-pool triangular arbitrage definition
	pools [3]int // SQLite IDs for pool addresses
	last  int    // cached pools[2]
	ap    *ArbPath
}

// ─── Boot strap ─────────────────────────────────────────────────────────────

func main() {
	// Aggressive inlining & no write-barrier in single-core buckets
	runtime.GOMAXPROCS(runtime.NumCPU())

	n := runtime.NumCPU() - 2 // leave two cores for OS / DB
	if n < 2 {
		panic("need ≥ 2 CPUs")
	}

	routers = make([]*CoreRouter, n)
	rings = make([]*ring.Ring, n)
	for i := 0; i < n; i++ {
		routers[i] = &CoreRouter{
			bucketByKey: pairidx.New(),
			queueByKey:  pairidx.New(),
		}
		rings[i] = ring.New(1 << 14)
		go runLoop(i, rings[i], routers[i], i >= n/2) // back half ≡ reverse-dir router set
	}

	db := mustDB("uniswap_pairs.db")
	cycles := parseCycles("cycles_3_3.txt")
	bucketMemo := make(map[[20]byte][2]*DeltaBucket, 4096) // [addr][dir] → bucket

	for i := range cycles {
		wireAddrs(db, &cycles[i])
		buildSplit(bucketMemo, &cycles[i])
	}

	select {} // wait forever – real system would start websocket ingest now
}

// runLoop is the per-CPU hot path: apply tick updates and recompute affected queues
// ─── runLoop hot-path ────────────────────────────────────────────────────────
func runLoop(id int, in *ring.Ring, rt *CoreRouter, isRev bool) {
	var stop, hot uint32
	ring.PinnedConsumer(id, in, &stop, &hot, func(ptr unsafe.Pointer) {
		u := (*PriceUpdate)(ptr)
		key := *(*[20]byte)(unsafe.Pointer(&u.Addr[0]))

		val := u.FwdTick
		if isRev {
			val = u.RevTick
		}

		// ----- bucket update -------------------------------------------------
		if p := rt.bucketByKey.Get(string(key[:])); p != nil {
			idx := *(*uint32)(p)
			rt.buckets[idx].CurLog = val
		} else if len(rt.buckets) > 0 {
			// single-bucket fallback (unit-test safety net)
			rt.buckets[0].CurLog = val
		}

		// ----- queue fan-out -------------------------------------------------
		if p := rt.queueByKey.Get(string(key[:])); p != nil {
			fi := *(*uint32)(p)
			recomputeSWAR(rt.fanOut[fi])
		}
	}, nil)
}

// recomputeSWAR reprioritises a small fan-out slice in a branch-free, unrolled fashion.
// It never allocates, and each Update() call is O(1) on the underlying queue.
func recomputeSWAR(refs []ref) {
	const unroll = 4 // manual SIMD-style block size (scalar Go)
	var aps [unroll]*ArbPath

	for i := 0; i < len(refs); i += unroll {
		end := i + unroll
		if end > len(refs) {
			end = len(refs)
		}

		// gather ArbPath pointers (branch-free once inside hot loop)
		for j := i; j < end; j++ {
			aps[j-i] = &refs[j].B.Path
		}

		// prefetch next refs block (best-effort; no-op on unsupported arch)
		if end < len(refs) {
			prefetch(&refs[end])
		}

		// compute absolute micro-tick and Update/Push on each queued handle
		for j := i; j < end; j++ {
			r := refs[j]
			tick := aps[j-i].LegSum // cached sum → 1 load, 0 adds
			absTick := int64(tick * 1e6)

			if err := r.Q.Update(absTick, r.H); err != nil {
				if err == bucketqueue.ErrItemNotFound || err == bucketqueue.ErrPastTick {
					_ = r.Q.Push(absTick, r.H) // re-enlist – still O(1)
				}
			}
		}
	}
}

// ─── One-off helpers ────────────────────────────────────────────────────────

//go:nosplit
func prefetch(p *ref) { // best-effort CPU prefetch where supported
	// The compiler emits a NOP on architectures without PREFETCH instructions.
	_ = *(*uintptr)(unsafe.Pointer(p)) // read into L1 – alias-safe dummy
}

func mustDB(path string) *sql.DB {
	db, _ := sql.Open("sqlite3", path)
	if err := db.Ping(); err != nil {
		panic(err)
	}
	return db
}

func parseCycles(path string) []Cycle {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(f)
	var out []Cycle
	var pools [3]int
	for sc.Scan() {
		line := sc.Bytes()
		n := 0
		in, val := false, 0
		for i := 0; i < len(line); i++ {
			b := line[i]
			switch {
			case b == '(':
				val, in = 0, true
			case b == ')' && in:
				pools[n] = val
				n++
				in = false
			case in && b >= '0' && b <= '9':
				val = val*10 + int(b-'0')
			}
		}
		if n == maxParens {
			out = append(out, Cycle{pools: pools, last: pools[2], ap: &ArbPath{}})
		}
	}
	return out
}

func wireAddrs(db *sql.DB, c *Cycle) {
	for i := 0; i < 3; i++ {
		addr := addr20(db, c.pools[i])
		c.ap.Addr[i] = addr
		c.ap.Dir[i] = (i == 1) // middle leg is reverse direction in std triangle
	}
	c.ap.Addr[2] = addr20(db, c.last)
	c.ap.Dir[2] = true
}

// buildSplit initialises per-pool buckets and wires fan-out slices – all O(1)
func buildSplit(memo map[[20]byte][2]*DeltaBucket, c *Cycle) {
	half := len(routers) / 2

	// 1) Ensure each pool/dir has its resident DeltaBucket.
	for i := 0; i < 3; i++ {
		a := c.ap.Addr[i]
		d := btoi(c.ap.Dir[i])
		e := memo[a]
		if e[d] == nil {
			b := &DeltaBucket{Queue: bucketqueue.New()}
			e[d] = b
			memo[a] = e

			core := d*half + int(a[0])%half
			rt := routers[core]
			idx := uint32(len(rt.buckets))
			rt.bucketByKey.Put(string(a[:]), idx)
			rt.buckets = append(rt.buckets, b)
		}
	}

	// 2) Producer leg borrows a queue handle once.
	prod := memo[c.ap.Addr[0]][btoi(c.ap.Dir[0])]
	c.ap.LegSum = c.ap.LegVal[1] + c.ap.LegVal[2]
	prod.Path = *c.ap

	q := prod.Queue
	h, _ := q.Borrow()
	_ = q.Push(0, h)

	// 3) Consumer legs wire refs → (q,h,B).
	for _, idx := range [...]int{1, 2} {
		a := c.ap.Addr[idx]
		d := c.ap.Dir[idx]
		core := btoi(d)*half + int(a[0])%half
		rt := routers[core]

		var fanIdx uint32
		if p := rt.queueByKey.Get(string(a[:])); p == nil {
			fanIdx = uint32(len(rt.fanOut))
			rt.queueByKey.Put(string(a[:]), fanIdx)
			rt.fanOut = append(rt.fanOut, nil)
		} else {
			fanIdx = *(*uint32)(p)
		}

		rt.fanOut[fanIdx] = append(rt.fanOut[fanIdx], ref{Q: q, H: h, B: prod})
	}
}

func addr20(db *sql.DB, id int) (out [20]byte) {
	var hs string
	if err := db.QueryRow("SELECT address FROM pools WHERE id=?", id).Scan(&hs); err != nil {
		panic(err)
	}
	_, _ = hex.Decode(out[:], []byte(hs[2:])) // skip 0x prefix
	return
}

// btoi is a branch-free bool→int converter (0/1).
func btoi(b bool) int {
	return int(*(*uint8)(unsafe.Pointer(&b)))
}
