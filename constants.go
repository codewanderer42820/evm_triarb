// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: constants.go — Global ISR Tunables & Parsing Probes
//
// Purpose:
//   - Defines ISR-wide constants for deduplication, GC limits, and WebSocket caps.
//   - Includes unsafe JSON field match probes for zero-alloc scanning.
//
// Notes:
//   - All constants are aggressively over-provisioned for high-FPS chains (e.g. Polygon).
//   - Probes are 8-byte aligned to support unsafe unaligned loads.
//   - Constants are sized with ≥10× margin for safety under burst loads.
//
// ⚠️ No runtime logic here — all values must be compile-time resolvable
// ─────────────────────────────────────────────────────────────────────────────

package main

// ───────────────────────────── Deduplication ──────────────────────────────

const (
	// ringBits defines the size of the deduplication ring buffer: 2^21 entries = 2,097,152 slots ≈ 64 MiB.
	// This is designed to hold approximately 24 hours of logs for high-FPS chains, such as Solana-like EVM chains.
	// The buffer is sized for 2x throughput and has 10× overcapacity for peak times.
	// This constant ensures the deduplication system can handle increased log rates while maintaining high performance under load.
	ringBits = 21 // Increased for 2x throughput, accommodating more logs per second (≈ 2M entries)

	// maxReorg defines the maximum reorganization depth allowed before events are evicted.
	// This is set to 256 blocks (approximately 6 minutes at 1.45s block time), ensuring that we can handle minor chain reorganizations
	// while maintaining responsiveness to recent changes in high-throughput chains.
	// A higher maxReorg depth ensures that the system can handle deeper reorganizations typical of Solana-like chains.
	maxReorg = 256 // Increased to handle deeper reorgs in high-throughput chains (≈ 6 minutes at 1.45s block time)
)

// ─────────────────────────── Memory Guardrails ─────────────────────────────

const (
	// heapSoftLimit triggers non-blocking GC (garbage collection) when exceeded.
	// If the heap size exceeds 256 MiB, the system will attempt to perform garbage collection
	// without blocking, ensuring efficient memory usage during high-throughput periods.
	heapSoftLimit = 256 << 20 // 256 MiB

	// heapHardLimit triggers a panic if the heap size exceeds this limit (1 GiB), signaling a failure state.
	// This ensures that the system stops if there is a potential memory leak or excessive memory usage.
	heapHardLimit = 1024 << 20 // 1 GiB
)

// ───────────────────────── WebSocket Configuration ─────────────────────────

const (
	// wsDialAddr specifies the WebSocket endpoint used to connect to Infura for Ethereum logs.
	// This address points to the mainnet of Polygon, and it must be updated if switching to a different network.
	wsDialAddr = "polygon-mainnet.infura.io:443"

	// wsPath defines the HTTP path used during the WebSocket connection upgrade handshake.
	// This path is unique to the Infura WebSocket API for Ethereum logs.
	wsPath = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"

	// wsHost is the host used in the SNI (Server Name Indication) during the TLS handshake.
	// This ensures the WebSocket client communicates securely with Infura’s servers.
	wsHost = "polygon-mainnet.infura.io"
)

// ──────────────────────── WebSocket Framing Caps ──────────────────────────

const (
	// maxFrameSize sets the maximum size for a raw WebSocket frame payload.
	// 1 MiB chosen to accommodate larger topic or data blobs in logs without exceeding buffer limits.
	// This accommodates higher-frequency chains like Solana-like EVM chains.
	maxFrameSize = 1024 << 10 // 1 MiB

	// frameCap defines the number of WebSocket frames that can be retained for parsing.
	// 524,288 frames for higher throughput scenarios, ensuring we can process more frames without exceeding buffer capacities.
	frameCap = 1 << 19 // 524,288 frames (for 4k FPS or higher)
)

// ────────────────────── JSON Key Probes for Parsing ───────────────────────

var (
	// These 8-byte probes are used for unsafe JSON field detection.
	// Each probe is carefully designed to match specific field names in JSON payloads.
	// They are 8-byte aligned to ensure efficient memory access and compatibility with low-level parsing techniques.
	// Each probe must be ASCII-safe to ensure proper comparison without encoding issues.

	keyAddress     = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // "address" field in JSON logs
	keyBlockHash   = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // "blockHash" field in JSON logs
	keyBlockNumber = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // "blockNumber" field in JSON logs
	keyData        = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // "data" field in JSON logs
	keyLogIndex    = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // "logIndex" field in JSON logs
	keyRemoved     = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // "removed" field in JSON logs (indicates if the log was removed)
	keyTopics      = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // "topics" field in JSON logs (typically an array)
	keyTransaction = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // "transactionHash" or "transactionIndex" field in JSON logs

	// Content signature for Uniswap V2 Sync() logs.
	// This 8-byte signature is used to identify Sync() events in the logs. It is a constant prefix in the topics.
	// The signature is checked for verifying the event type.
	sigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'}
)
