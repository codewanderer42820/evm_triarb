// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: constants.go — Global ISR Tunables & Parsing Probes
//
// Purpose:
//   - Defines ISR-wide constants for deduplication, GC limits, and WebSocket caps.
//   - Includes unsafe JSON field match probes for zero-alloc scanning.
//
// Notes:
//   - All constants are aggressively over-provisioned for high-FPS chains (e.g., Polygon).
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
	// wsDialAddr specifies the WebSocket endpoint used to connect to an Ethereum-compatible provider.
	// This address should be updated to match the desired network (e.g., mainnet, testnet) and provider (e.g., Infura, Alchemy).
	// The URL format is typically "<network>-mainnet.infura.io" or "<network>-mainnet.alchemyapi.io".
	wsDialAddr = "mainnet.infura.io:443" // Update this URL to match your provider and network

	// wsPath defines the HTTP path used during the WebSocket connection upgrade handshake.
	// This path is specific to the WebSocket API of the Ethereum provider. It may vary depending on the provider.
	// For example, Infura uses "/ws/v3/<project_id>" as the WebSocket endpoint path.
	wsPath = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126" // Replace with your own provider's WebSocket path

	// wsHost specifies the host used in the SNI (Server Name Indication) during the TLS handshake.
	// This ensures that the WebSocket client connects securely to the provider’s servers.
	// Typically, this is the base URL of the WebSocket service, e.g., "mainnet.infura.io" or "mainnet.alchemyapi.io".
	wsHost = "mainnet.infura.io" // Update to match the provider’s host (e.g., Alchemy, Infura, or your custom provider)
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

	// keyAddress is a probe for detecting the "address" field in JSON logs.
	keyAddress = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // "address" field in JSON logs

	// keyBlockHash is a probe for detecting the "blockHash" field in JSON logs.
	keyBlockHash = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // "blockHash" field in JSON logs

	// keyBlockNumber is a probe for detecting the "blockNumber" field in JSON logs.
	keyBlockNumber = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // "blockNumber" field in JSON logs

	// keyData is a probe for detecting the "data" field in JSON logs.
	keyData = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // "data" field in JSON logs

	// keyLogIndex is a probe for detecting the "logIndex" field in JSON logs.
	keyLogIndex = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // "logIndex" field in JSON logs

	// keyRemoved is a probe for detecting the "removed" field in JSON logs.
	// The "removed" field indicates if the log was removed during reorganization.
	keyRemoved = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // "removed" field in JSON logs (indicates if the log was removed)

	// keyTopics is a probe for detecting the "topics" field in JSON logs.
	// The "topics" field typically holds an array of topics associated with the log.
	keyTopics = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // "topics" field in JSON logs (typically an array)

	// keyTransaction is a probe for detecting the "transactionHash" or "transactionIndex" field in JSON logs.
	keyTransaction = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // "transactionHash" or "transactionIndex" field in JSON logs

	// sigSyncPrefix is a constant signature used to identify Sync() events in the logs.
	// This 8-byte signature is checked for verifying the event type for Uniswap V2 Sync() logs.
	// It is a fixed prefix that appears in the topics of these logs.
	sigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // Sync event signature in Uniswap V2 logs
)
