package main

// constants.go — build-wide tunables and shared literals.
//
// All limits are now *redundancy‑heavy* — roughly **10×** the busiest Polygon day
// we have observed. Each constant includes a rationale so future tuning is
// self‑documenting.

const (
	// ────────────────────── Deduplication ring ──────────────────────
	ringBits = 18 // 2^18 = 262 144 slots (≈ 8 MiB) → ~24 h of logs with 10× head‑room
	maxReorg = 64 // allow chain re‑orgs up to 64 blocks (~13 min on Polygon)

	// ─────────────────────── Heap guard‑rails ───────────────────────
	heapSoftLimit = 128 << 20 // 128 MiB — trigger manual GC when exceeded
	heapHardLimit = 512 << 20 // 512 MiB — panic; indicates a leak or run‑away backlog

	// ───── WebSocket endpoint (Infura Polygon main‑net, primary) ─────
	// NOTE: fall‑back hosts can be added in code if desired.
	wsDialAddr = "polygon-mainnet.infura.io:443"
	wsPath     = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126" // TODO: pull from $INFURA_KEY
	wsHost     = "polygon-mainnet.infura.io"

	// ────────────────────── WebSocket framing ───────────────────────
	maxFrameSize = 512 << 10 // 512 KiB — aligns with Infura’s internal slice cap
	frameCap     = 1 << 18   // 262 144 decoded frames → ~2 min backlog at 2 kfps
)

// Eight-byte aligned probes scanned by the zero-alloc JSON parser.
var (
	keyAddress          = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'}
	keyData             = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'}
	keyTopics           = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'}
	keyBlockNumber      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'}
	keyTransactionIndex = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'}
	keyLogIndex         = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'}

	sigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'}
)
