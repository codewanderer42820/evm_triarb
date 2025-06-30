package main

// constants.go — global runtime tunables and event field probes.
//
// All constants are **intentionally oversized** for high-volume production use.
// Sizing rationale is documented to justify over-provisioning.

// ───────────────────────────── Deduplication ──────────────────────────────

const (
	// ringBits controls the deduper ring size as 2^N slots.
	// 2^18 = 262,144 entries = 8 MiB → stores ~24h of Polygon logs with 10× headroom.
	ringBits = 18

	// maxReorg defines how deep a chain re-org can go before we discard history.
	// Polygon rarely reorgs >3–4 blocks; 64 ensures 13+ minutes of cushion.
	maxReorg = 64
)

// ─────────────────────────── Memory Guardrails ─────────────────────────────

const (
	// heapSoftLimit triggers a manual GC cycle (non-blocking).
	// Used to proactively trim memory under sustained pressure.
	heapSoftLimit = 128 << 20 // 128 MiB

	// heapHardLimit triggers a panic if exceeded.
	// Indicates a logic leak or infinite backlog. Safe to kill.
	heapHardLimit = 512 << 20 // 512 MiB
)

// ───────────────────────── WebSocket Configuration ─────────────────────────

const (
	// wsDialAddr is the raw TCP dial target (no scheme).
	// If running through Infura or proxy, ensure host+port match.
	wsDialAddr = "polygon-mainnet.infura.io:443"

	// wsPath is the request path used during WebSocket upgrade.
	// This embeds a project key inline; may be made configurable via env.
	wsPath = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"

	// wsHost is used for TLS SNI — required for most CDN-backed endpoints.
	wsHost = "polygon-mainnet.infura.io"
)

// ──────────────────────── WebSocket Framing Caps ──────────────────────────

const (
	// maxFrameSize defines the buffer size for incoming WebSocket frames.
	// Should be ≥ Infura's largest observed frame payload (max topics, logs).
	maxFrameSize = 512 << 10 // 512 KiB

	// frameCap controls the number of parsed WebSocket frames stored in ring.
	// 262,144 entries cover ~2 minutes @ 2k FPS.
	frameCap = 1 << 18 // 262,144
)

// ────────────────────── JSON Key Probes for Parsing ───────────────────────
//
// These 8-byte ASCII probes are used in unsafe scanning to match JSON keys.
// Each one is loaded with an unaligned 8-byte read and must be ≥8 bytes.
// These tags MUST be ASCII-safe and match only valid JSON field prefixes.

var (
	// Matches: `"address":"0xabc..."`
	keyAddress = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'}

	// Matches: `"data":"0xabc..."`
	keyData = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'}

	// Matches: `"topics":[...`
	keyTopics = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'}

	// Matches: `"blockNumber":"0x..."`
	keyBlockNumber = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'}

	// Prefix match for `"transactionIndex":...`, validated via full 18B literal.
	keyTransactionIndex = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'}

	// Matches: `"logIndex":"0x..."`
	keyLogIndex = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'}

	// sigSyncPrefix is used to quickly filter only Sync() logs from Uniswap V2.
	// This is a content-based tag, not a JSON field name.
	sigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // topic0[3]
)
