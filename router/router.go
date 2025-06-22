package router

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"log"
	"math/rand"
	"os"
	"runtime"
	"time"
	"unsafe"

	_ "github.com/mattn/go-sqlite3"

	"main/bucketqueue"
	"main/pairidx"
	"main/ring"
)

// ────────────────────────── Direction
type Direction byte

const (
	Fwd Direction = 'F' // token0 → token1
	Rev Direction = 'R' // token1 → token0
)

// ────────────────────────── Key table (copy‑free)
var keyTable = make(map[[20]byte][2]string)

func makeKey(a [20]byte, d Direction) [21]byte {
	var k [21]byte
	copy(k[:20], a[:])
	k[20] = byte(d)
	return k
}

func buildKeys(a [20]byte) {
	kF := makeKey(a, Fwd)
	kR := makeKey(a, Rev)
	keyTable[a] = [2]string{string(kF[:]), string(kR[:])}
}

//go:nosplit
func keyOf(a *[20]byte, d Direction) string {
	return keyTable[*a][btoi(d == Rev)]
}

// ────────────────────────── Hot structs
type ArbPath struct {
	LegPtr [3]*float64
	Addr   [3][20]byte
	Dir    [3]Direction
	Anchor uint8
	_pad   [4]byte
} // 88 B

type DeltaBucket struct {
	Queue  *bucketqueue.Queue
	CurLog float64
	Addr   [20]byte
	Dir    Direction
	Core   int // owning core index for fan‑out affinity
	//lint:ignore U1000 padding for alignment
	_pad [4]byte
} // 48 B

type ref struct {
	H bucketqueue.Handle
	Q *bucketqueue.Queue
} // use Handle instead of *Node

type CoreRouter struct {
	bucketByKey *pairidx.HashMap
	buckets     []*DeltaBucket
	queueByKey  *pairidx.HashMap
	fanOut      [][]ref
}

type PriceUpdate struct {
	FwdTick, RevTick float64
	Addr             [20]byte
	_pad             [4]byte
}

// ────────────────────────── Bootstrap
func init() {
	workers := runtime.NumCPU() - 4
	if workers < 1 {
		log.Fatal("need >4 CPUs")
	}
	db := mustDB("uniswap_pairs.db")
	routers := make([]*CoreRouter, workers)
	rings := make([]*ring.Ring, workers)
	for i := range routers {
		routers[i] = &CoreRouter{bucketByKey: pairidx.New(), queueByKey: pairidx.New()}
		rings[i] = ring.New(1 << 14)
	}

	buckets := map[[21]byte]*DeltaBucket{}
	rand.Seed(time.Now().UnixNano())

	for _, p := range parseCycles("cycles_3_3.txt") {
		wireAddrs(db, p)
		build(workers, buckets, routers, p)
	}

	var stop, hot uint32
	for cid := range routers {
		go runLoop(cid, rings[cid], &stop, &hot, routers[cid])
	}
}

// ────────────────────────── Consumer
func runLoop(cid int, in *ring.Ring, stop, hot *uint32, rt *CoreRouter) {
	ring.PinnedConsumer(cid, in, stop, hot, func(ptr unsafe.Pointer) {
		u := (*PriceUpdate)(ptr)
		keys := keyTable[u.Addr]
		if idx, ok := rt.bucketByKey.Get(keys[0]); ok {
			rt.buckets[idx].CurLog = u.FwdTick
		}
		if idx, ok := rt.bucketByKey.Get(keys[1]); ok {
			rt.buckets[idx].CurLog = u.RevTick
		}
		for _, k := range keys {
			if fi, ok := rt.queueByKey.Get(k); ok {
				recompute(rt.fanOut[fi])
			}
		}
	}, nil)
}

//go:nosplit
func recompute(refs []ref) {
	for _, r := range refs {
		n := &r.Q.Arena[r.H]
		ap := (*ArbPath)(unsafe.Pointer(n))
		tick := *ap.LegPtr[(ap.Anchor+1)%3] + *ap.LegPtr[(ap.Anchor+2)%3]
		_ = r.Q.Update(int64(tick*1e6), r.H)
	}
}

// ────────────────────────── Init helpers
func mustDB(p string) *sql.DB {
	db, err := sql.Open("sqlite3", p)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func parseCycles(f string) (out []struct {
	pools [3]int
	last  int
	ap    *ArbPath
}) {
	file, err := os.Open(f)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	sc := bufio.NewScanner(file)
	for sc.Scan() {
		ids := parseLine(sc.Bytes())
		out = append(out, struct {
			pools [3]int
			last  int
			ap    *ArbPath
		}{[3]int{ids[0], ids[1], ids[2]}, ids[3], &ArbPath{Dir: [3]Direction{Fwd, Fwd, Fwd}}})
	}
	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
	return
}

//go:nosplit
func parseLine(b []byte) (ids [4]int) {
	var n, idx int
	in := false
	for _, c := range b {
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			if in {
				n = n*10 + int(c-'0')
			}
		case '(':
			n, in = 0, true
		case ')':
			if in {
				if idx < 4 {
					ids[idx] = n
				}
				idx++
				n, in = 0, false
			}
		}
	}
	if idx == 3 {
		ids[3] = ids[0]
	}
	return
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func addr20(db *sql.DB, id int) (out [20]byte) {
	var hs string
	if err := db.QueryRow("SELECT address FROM pools WHERE id=?", id).Scan(&hs); err != nil {
		log.Fatal(err)
	}
	if _, err := hex.Decode(out[:], []byte(hs[2:])); err != nil {
		log.Fatal(err)
	}
	return
}

func wireAddrs(db *sql.DB, p struct {
	pools [3]int
	last  int
	ap    *ArbPath
}) {
	for i, id := range p.pools {
		a := addr20(db, id)
		copy(p.ap.Addr[i][:], a[:])
		if _, ok := keyTable[a]; !ok {
			buildKeys(a)
		}
	}
	a := addr20(db, p.last)
	copy(p.ap.Addr[2][:], a[:])
}

func build(workers int, memo map[[21]byte]*DeltaBucket, rt []*CoreRouter, p struct {
	pools [3]int
	last  int
	ap    *ArbPath
}) {
	for leg := 0; leg < 3; leg++ {
		anchorCore := rand.Intn(workers)
		for _, dir := range [...]Direction{Fwd, Rev} {
			kArr := makeKey(p.ap.Addr[leg], dir)
			key := keyOf(&p.ap.Addr[leg], dir)
			bkt, ok := memo[kArr]
			if !ok {
				bkt = &DeltaBucket{Addr: p.ap.Addr[leg], Dir: dir, Queue: bucketqueue.New(), Core: anchorCore}
				memo[kArr] = bkt
				owner := rt[anchorCore]
				owner.bucketByKey.Put(key, uint16(len(owner.buckets)))
				owner.buckets = append(owner.buckets, bkt)
			}

			ap := *p.ap
			ap.Anchor = uint8(leg)
			for i := 0; i < 3; i++ {
				ap.LegPtr[i] = &memo[makeKey(ap.Addr[i], ap.Dir[i])].CurLog
			}

			h, _ := bkt.Queue.Borrow()
			*(*ArbPath)(unsafe.Pointer(&bkt.Queue.Arena[h])) = ap
			bkt.Queue.Push(0, h)

			owner := rt[bkt.Core]
			for _, o := range [...]int{(leg + 1) % 3, (leg + 2) % 3} {
				oKey := keyOf(&ap.Addr[o], ap.Dir[o])
				idx, ok := owner.queueByKey.Get(oKey)
				if !ok {
					idx = uint16(len(owner.fanOut))
					owner.queueByKey.Put(oKey, idx)
					owner.fanOut = append(owner.fanOut, nil)
				}
				owner.fanOut[idx] = append(owner.fanOut[idx], ref{Q: bkt.Queue, H: h})
			}
		}
	}
}
