package main

// constants.go — global runtime tunables and event field probes.
//
// All system constants are **over-provisioned** for production safety,
// set to roughly 10× the highest historical daily volume on Polygon.
// Each field includes a comment justifying its sizing.

// ───────────────────────────── Deduplication ──────────────────────────────

const (
	// ringBits controls the dedupe ring size.
	// 2^18 = 262,144 entries = 8 MiB. Stores ~24 hours of events with 10× overhead.
	ringBits = 18

	// maxReorg defines the tolerated chain re-org depth.
	// Polygon rarely re-orgs more than 3–4 blocks; we allow up to 64 (~13 min).
	maxReorg = 64
)

// ─────────────────────────── Memory Guardrails ─────────────────────────────

const (
	// heapSoftLimit triggers a manual GC pass if exceeded.
	// Allows short-term spikes while keeping pressure off runtime scheduler.
	heapSoftLimit = 128 << 20 // 128 MiB

	// heapHardLimit forces a panic if exceeded — indicates a leak or backlog.
	heapHardLimit = 512 << 20 // 512 MiB
)

// ───────────────────────── WebSocket Configuration ─────────────────────────

const (
	// wsDialAddr is the Infura Polygon mainnet endpoint (TCP-only, no scheme).
	// Additional fallbacks may be supported in calling code.
	wsDialAddr = "polygon-mainnet.infura.io:443"

	// wsPath is the full subscription path (project ID embedded).
	// Consider making this configurable via $INFURA_KEY.
	wsPath = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"

	// wsHost is the TLS SNI host. Required by most CDNs and TLS libraries.
	wsHost = "polygon-mainnet.infura.io"
)

// ──────────────────────── WebSocket Framing Caps ──────────────────────────

const (
	// maxFrameSize controls how much raw WebSocket payload is buffered.
	// Must match or exceed expected Infura payload caps.
	maxFrameSize = 512 << 10 // 512 KiB

	// frameCap controls how many parsed frames can live in ring at once.
	// 262,144 frames supports 2 minutes at 2,000 fps.
	frameCap = 1 << 18 // 262,144
)

// ────────────────────── JSON Key Probes for Parsing ───────────────────────
//
// These are 8-byte probes used to quickly locate event fields inside JSON.
// Aligned memory loads (uint64) enable fast skip scans. Each must be
// length ≥ 8 and ASCII-safe.

var (
	keyAddress          = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'}
	keyData             = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'}
	keyTopics           = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'}
	keyBlockNumber      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'}
	keyTransactionIndex = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'}
	keyLogIndex         = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'}

	// sigSyncPrefix is the prefix of the Uniswap V2 Sync() event signature hash.
	// Used to distinguish relevant events early in parser fast path.
	sigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'}
)
