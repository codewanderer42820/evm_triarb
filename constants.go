// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: constants.go — Global ISR Tunables & Parsing Probes
//
// Purpose:
//   - Defines ISR-wide constants for deduplication, GC limits, and WebSocket caps
//   - Includes unsafe JSON field match probes for zero-alloc scanning
//
// Notes:
//   - All constants are aggressively over-provisioned for high-FPS chains (e.g. Polygon)
//   - Probes are 8-byte aligned to support unsafe unaligned loads
//   - Constants are sized with ≥10× margin for safety under burst loads
//
// ⚠️ No runtime logic here — all values must be compile-time resolvable
// ─────────────────────────────────────────────────────────────────────────────

package main

// ───────────────────────────── Deduplication ──────────────────────────────

const (
	// ringBits sets deduper size: 2^18 = 262,144 entries = 8 MiB
	// Covers ~24h of Polygon logs @ peak, with 10× overcapacity
	ringBits = 18

	// maxReorg is the max reorg depth tolerated by deduper before eviction
	// 64 blocks = ~13 min @ Polygon's 2.9s block time
	maxReorg = 64
)

// ─────────────────────────── Memory Guardrails ─────────────────────────────

const (
	// heapSoftLimit triggers non-blocking GC when exceeded
	heapSoftLimit = 128 << 20 // 128 MiB

	// heapHardLimit triggers panic — ISR logic considered failed
	heapHardLimit = 512 << 20 // 512 MiB
)

// ───────────────────────── WebSocket Configuration ─────────────────────────

const (
	// wsDialAddr is the TCP address used for Infura dial (no schema)
	wsDialAddr = "polygon-mainnet.infura.io:443"

	// wsPath is the HTTP upgrade path for WebSocket connection
	wsPath = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"

	// wsHost is the SNI hostname for TLS handshake
	wsHost = "polygon-mainnet.infura.io"
)

// ──────────────────────── WebSocket Framing Caps ──────────────────────────

const (
	// maxFrameSize is the raw payload buffer size
	// 512 KiB covers max topic/data bloat in logs (worst-case from Infura)
	maxFrameSize = 512 << 10 // 512 KiB

	// frameCap defines number of retained parsed log frames
	// Covers ~2 minutes @ 2k FPS
	frameCap = 1 << 18 // 262,144
)

// ────────────────────── JSON Key Probes for Parsing ───────────────────────

var (
	// These 8-byte probes are used in unsafe JSON field detection.
	// Each must be ASCII-safe and ≥8B to ensure alignment compatibility.

	keyAddress          = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'}
	keyData             = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'}
	keyTopics           = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'}
	keyBlockNumber      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'}
	keyTransactionIndex = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'}
	keyLogIndex         = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'}

	// Content signature for Uniswap V2 Sync() logs
	sigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'}
)
