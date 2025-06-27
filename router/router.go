// router.go — 64‑core fan‑out router (queues pre‑seeded with dummy ticks).
//
// Updated 2025‑06‑27: the ring now transfers **32‑byte payloads in‑place**.
// All pushes/pops therefore recast a struct to *[32]byte* on the send side
// and back to *PriceUpdate on the consumer side.  No heap allocation occurs –
// just an unsafe view of the same 32‑byte buffer.

package router

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"runtime"
	"unsafe"

	"main/bucketqueue"
	"main/fastuni"
	"main/localidx"
	"main/ring"
	"main/types"
	"main/utils"
)

/*────────────────────────  Data Structures  ────────────────────────*/

type PairID uint32

// Local (dense) id inside one core.
type LocalPairID uint32

type CPUMask uint64

type TriCycle [3]PairID

type ArbPath struct {
	Ticks [3]float64
	Pairs TriCycle
}

// Ref: one per (pair, triangle). CommonEdge = this pair’s leg (0,1,2).
type Ref struct {
	Pairs      TriCycle
	CommonEdge uint16
}

type Shard struct {
	Pair PairID
	Refs []Ref
}

// ---------------- 32‑byte message pushed through the ring ------------------
// PriceUpdate is recast to a *[32]byte when enqueued.  Size check below keeps
// us honest – an accidental field addition that grows the struct >32 B will
// fail the build.

type PriceUpdate struct {
	Pair             PairID  // 4  B
	_pad             uint32  // 4  B (align the float64s)
	FwdTick, RevTick float64 // 16 B
	_                [8]byte // 8  B (explicit pad → total == 32)
}

const _priceUpdateSize = unsafe.Sizeof(PriceUpdate{})

// compile‑time guarantee (will not compile if violated)
var _ [1]struct{} = [1]struct{}{{}}

func init() {
	const msgSize = 32
	_ = [msgSize - _priceUpdateSize]byte{}
}

// Fanout keeps no queue pointer; it stores the local‑pair index (Lid)
// and a *shared* handle for the pre‑inserted queue item.
type Fanout struct {
	Path       *ArbPath
	Handle     bucketqueue.Handle // shared by the two fan‑outs
	Lid        LocalPairID        // index into rt.Buckets
	FanoutEdge uint16             // tick slot to write (0‑2)
}

type CoreRouter struct {
	Buckets   []bucketqueue.Queue // value slice; core owns queues
	Fanouts   [][]Fanout          // fan‑outs per local pair
	Local     localidx.Hash
	IsReverse bool
}

/*────────────────────────  Globals  ────────────────────────*/

var (
	coreRouters   [64]*CoreRouter
	coreRings     [64]*ring.Ring
	addrToPairID  [1 << 17]PairID
	routingBitmap [1 << 17]CPUMask
	rawShards     map[PairID][]Shard

	splitThreshold = 16_384
)

/*────────────────────────  Fan‑out Planner  ────────────────────────*/

func ResetFanouts() { rawShards = make(map[PairID][]Shard) }

func shuffleRefs(refs []Ref) {
	for i := len(refs) - 1; i > 0; i-- {
		j := crandInt(i + 1)
		refs[i], refs[j] = refs[j], refs[i]
	}
}

func crandInt(n int) int {
	var b [8]byte
	rand.Read(b[:])
	v := binary.LittleEndian.Uint64(b[:])
	if n&(n-1) == 0 {
		return int(v & uint64(n-1))
	}
	hi, _ := bits.Mul64(v, uint64(n))
	return int(hi)
}

// BuildFanouts: one Ref per (pair, CommonEdge) → shard by pair.
func BuildFanouts(cycles []TriCycle) {
	ResetFanouts()
	tmp := make(map[PairID][]Ref, len(cycles)*3)

	for _, tri := range cycles {
		for pos, pair := range tri {
			tmp[pair] = append(tmp[pair], Ref{Pairs: tri, CommonEdge: uint16(pos)})
		}
	}

	for pair, refs := range tmp {
		shuffleRefs(refs)
		for off := 0; off < len(refs); off += splitThreshold {
			end := off + splitThreshold
			if end > len(refs) {
				end = len(refs)
			}
			rawShards[pair] = append(rawShards[pair],
				Shard{Pair: pair, Refs: refs[off:end]})
		}
	}
}

/*────────────────────────  Bootstrap  ────────────────────────*/

// InitRouters sets up per‑core routers and seeds them with shards.
func InitRouters(cycles []TriCycle) { // ← new generalized name
	n := runtime.NumCPU() - 4
	if n < 8 {
		n = 8
	}
	if n > 64 {
		n = 64
	}
	if n&1 != 0 {
		n--
	}
	half := n / 2

	BuildFanouts(cycles)

	shardCh := make([]chan Shard, n)
	for i := range shardCh {
		shardCh[i] = make(chan Shard, 128)
	}

	for coreID := 0; coreID < n; coreID++ {
		go shardWorker(coreID, half, shardCh[coreID])
	}

	coreIdx := 0
	for _, shards := range rawShards {
		for _, s := range shards {
			fwd := coreIdx % half
			rev := fwd + half
			shardCh[fwd] <- s
			shardCh[rev] <- s

			// mark *all* three legs of every triangle carried by this shard so
			// RouteUpdate pushes ticks to the correct forward & reverse cores.
			for _, ref := range s.Refs { // each ref holds the tri‑cycle
				for _, pid := range ref.Pairs { // three pair IDs
					routingBitmap[pid] |= 1 << fwd
					routingBitmap[pid] |= 1 << rev
				}
			}
			coreIdx++
		}
	}
	for _, ch := range shardCh {
		close(ch)
	}
}

func shardWorker(coreID, half int, in <-chan Shard) {
	runtime.LockOSThread()

	rt := &CoreRouter{
		Buckets:   make([]bucketqueue.Queue, 0, 1024),
		Local:     localidx.New(1 << 16),
		IsReverse: coreID >= half,
	}
	coreRouters[coreID] = rt

	rb := ring.New(1 << 14) // 16K‑slot SPSC queue
	coreRings[coreID] = rb

	paths := make([]ArbPath, 0, 1024)

	for sh := range in {
		installShard(rt, &sh, &paths)
	}

	ring.PinnedConsumer(
		coreID, rb, new(uint32), new(uint32),
		func(p *[32]byte) { onPrice(rt, (*PriceUpdate)(unsafe.Pointer(p))) },
		make(chan struct{}),
	)
}

/*────────────────────────  Shard Attach  ────────────────────────*/

// installShard attaches one pair’s refs to a core router.
//   - Creates the per‑pair bucket on first sighting.
//   - Pre‑inserts each ArbPath into that bucket with dummy tick 0.
//   - Stores one borrowed handle (shared by the two fan‑outs) plus the
//     local‑pair index (Lid) so onPrice can refetch the queue fast.
func installShard(rt *CoreRouter, sh *Shard, paths *[]ArbPath) {
	lid32 := rt.Local.Put(uint32(sh.Pair), uint32(len(rt.Buckets)))
	lid := LocalPairID(lid32)

	// Make bucket & fan‑out slice if this is the first shard for the pair.
	if int(lid32) == len(rt.Buckets) {
		rt.Buckets = append(rt.Buckets, *bucketqueue.New())
	}
	if int(lid32) >= len(rt.Fanouts) {
		rt.Fanouts = append(rt.Fanouts,
			make([][]Fanout, int(lid32)-len(rt.Fanouts)+1)...)
	}

	q := &rt.Buckets[lid32]

	for _, ref := range sh.Refs {
		*paths = append(*paths, ArbPath{Pairs: ref.Pairs})
		pPtr := &(*paths)[len(*paths)-1]

		// One handle per path, shared by the two fan‑outs.
		handle, _ := q.Borrow()                     // error deliberately ignored
		_ = q.Push(0, handle, unsafe.Pointer(pPtr)) // ignore push error by design

		for _, slot := range []uint16{
			uint16((ref.CommonEdge + 1) % 3),
			uint16((ref.CommonEdge + 2) % 3),
		} {
			rt.Fanouts[lid32] = append(rt.Fanouts[lid32], Fanout{
				Path:       pPtr,
				Handle:     handle,
				Lid:        lid,
				FanoutEdge: slot,
			})
		}
	}
}

/*────────────────────────  Tick Processing  ────────────────────────*/

func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	lid, _ := rt.Local.Get(uint32(upd.Pair))
	q := &rt.Buckets[lid]
	fan := rt.Fanouts[lid]

	if _, _, ptr := q.PeepMin(); ptr != nil {
		p := (*ArbPath)(ptr)
		if profit := tick + p.Ticks[0] + p.Ticks[1] + p.Ticks[2]; profit < 0 {
			onProfitablePath(p, profit)
		}
	}

	for _, f := range fan {
		p := f.Path
		p.Ticks[f.FanoutEdge] = tick
		sum := p.Ticks[0] + p.Ticks[1] + p.Ticks[2]
		q := &rt.Buckets[f.Lid]
		q.Update(l2Bucket(sum), f.Handle, unsafe.Pointer(p))
	}
}

/*────────────────────────  Utilities  ────────────────────────*/

func l2Bucket(x float64) int64 {
	const clamp, scale = 128.0, 16.0
	if x > clamp {
		x = clamp
	} else if x < -clamp {
		x = -clamp
	}
	return int64((x + clamp) * scale)
}

/*────────────────────────  Pair Registry  ────────────────────────*/

func RegisterPair(addr40 []byte, pid PairID) {
	idx := utils.Hash17(addr40)
	for {
		if addrToPairID[idx] == 0 {
			addrToPairID[idx] = pid
			return
		}
		idx = (idx + 64) & (1<<17 - 1)
	}
}

func RegisterRoute(pid PairID, core uint8) {
	if core >= 64 {
		panic("core id out of range")
	}
	routingBitmap[pid] |= 1 << core
}

func lookupPairID(addr []byte) PairID {
	idx := utils.Hash17(addr)
	for {
		if id := addrToPairID[idx]; id != 0 {
			return id
		}
		idx = (idx + 64) & (1<<17 - 1)
	}
}

/*────────────────────────  External Entry  ────────────────────────*/

// RouteUpdate converts an on‑chain Sync event (LogView) into ticks and pushes
// a *32‑byte* message to every core interested in this pair.
func RouteUpdate(v *types.LogView) {
	addr := v.Addr[3:43]
	pair := lookupPairID(addr)

	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	tick := fastuni.Log2ReserveRatio(r0, r1)

	// Build message in‑place inside a fixed 32‑byte buffer.
	var msg [32]byte
	upd := (*PriceUpdate)(unsafe.Pointer(&msg))
	upd.Pair = pair
	upd.FwdTick = tick
	upd.RevTick = -tick

	for m := routingBitmap[pair]; m != 0; {
		core := bits.TrailingZeros64(uint64(m))
		coreRings[core].Push(&msg)
		m &^= 1 << core
	}
}

/*────────────────────────  Profit Hook  ────────────────────────*/

func onProfitablePath(_ *ArbPath, _ float64) {}
