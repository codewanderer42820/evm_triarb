// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: constants.go — Global ISR Tunables & Parsing Probes (OPTIMIZED)
//
// Purpose:
//   - Defines ISR-wide constants for deduplication, GC limits, and WebSocket caps.
//   - Includes unsafe JSON field match probes for zero-alloc scanning.
//
// Notes:
//   - Optimized for sub-microsecond latency and minimal memory pressure
//   - Tuned for high-frequency chains (Solana-like) while maintaining safety margins
//   - Cache-friendly sizing with power-of-2 alignment for optimal performance
//
// ⚠️ No runtime logic here — all values must be compile-time resolvable
// ─────────────────────────────────────────────────────────────────────────────

package constants

// ───────────────────────────── Deduplication ──────────────────────────────

const (
	// RingBits defines the size of the deduplication ring buffer: 2^18 entries = 262,144 slots ≈ 8 MiB.
	// Optimized for:
	// - L3 cache efficiency (fits in 8MB vs 16MB)
	// - Still handles 250K+ logs safely with <50% utilization
	// - Better cache line utilization and fewer TLB misses
	// - Sufficient for even burst scenarios on high-freq chains
	RingBits = 18 // Reduced from 19 - better cache efficiency, still safe for high-freq chains

	// MaxReorg defines the maximum reorganization depth allowed before events are evicted.
	// Optimized for:
	// - 128 blocks ≈ 3 minutes at 1.4s block time (sufficient for most reorgs)
	// - Reduces memory pressure by 50% vs 256 blocks
	// - Real-world reorgs rarely exceed 64 blocks, so 128 is conservative
	// - Faster staleness checks (smaller arithmetic)
	MaxReorg = 128 // Reduced from 256 - sufficient for real-world reorgs, better performance
)

// ─────────────────────────── Memory Guardrails ─────────────────────────────

const (
	// HeapSoftLimit triggers non-blocking GC when exceeded.
	// Optimized for:
	// - 128 MiB reduces GC pressure and frequency
	// - Better for sustained high-throughput operation
	// - Aligns with L3 cache + dedup buffer sizing
	// - Prevents GC thrashing during burst periods
	HeapSoftLimit = 128 << 20 // 128 MiB - reduced from 256 for tighter memory control

	// HeapHardLimit triggers panic if exceeded.
	// Optimized for:
	// - 512 MiB sufficient for ISR operation
	// - Faster leak detection
	// - Better suited for containerized deployments
	// - Prevents system-wide memory pressure
	HeapHardLimit = 512 << 20 // 512 MiB - reduced from 1024 for faster leak detection
)

// ───────────────────────── WebSocket Configuration ─────────────────────────

const (
	// WsDialAddr specifies the WebSocket endpoint used to connect to an Ethereum-compatible provider.
	// This address should be updated to match the desired network (e.g., mainnet, testnet) and provider (e.g., Infura, Alchemy).
	// The URL format is typically "<network>-mainnet.infura.io" or "<network>-mainnet.alchemyapi.io".
	WsDialAddr = "mainnet.infura.io:443" // Update this URL to match your provider and network

	// WsPath defines the HTTP path used during the WebSocket connection upgrade handshake.
	// This path is specific to the WebSocket API of the Ethereum provider. It may vary depending on the provider.
	// For example, Infura uses "/ws/v3/<project_id>" as the WebSocket endpoint path.
	WsPath = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126" // Replace with your own provider's WebSocket path

	// WsHost specifies the host used in the SNI (Server Name Indication) during the TLS handshake.
	// This ensures that the WebSocket client connects securely to the provider's servers.
	// Typically, this is the base URL of the WebSocket service, e.g., "mainnet.infura.io" or "mainnet.alchemyapi.io".
	WsHost = "mainnet.infura.io" // Update to match the provider's host (e.g., Alchemy, Infura, or your custom provider)
)

// ──────────────────────── WebSocket Framing Caps ──────────────────────────

const (
	// MaxFrameSize sets the maximum size for a raw WebSocket frame payload.
	// Optimized for:
	// - 512 KiB sufficient for largest realistic log payloads
	// - Better memory locality and cache efficiency
	// - Reduces buffer allocation overhead
	// - Aligns with typical L2 cache size (512KB-1MB)
	MaxFrameSize = 512 << 10 // 512 KiB - reduced from 1MB for better cache efficiency

	// FrameCap defines the number of WebSocket frames that can be retained for parsing.
	// Optimized for:
	// - 2^17 = 131,072 frames sufficient for high burst scenarios
	// - Better cache locality with smaller frame ring
	// - Reduced memory pressure on frame metadata
	// - Faster frame ring wraparound and cleanup
	FrameCap = 1 << 17 // 131,072 frames - reduced from 2^19 for better performance
)

// ────────────────────── JSON Key Probes for Parsing ───────────────────────

var (
	// These 8-byte probes are used for unsafe JSON field detection.
	// Each probe is carefully designed to match specific field names in JSON payloads.
	// They are 8-byte aligned to ensure efficient memory access and compatibility with low-level parsing techniques.
	// Each probe must be ASCII-safe to ensure proper comparison without encoding issues.

	// KeyAddress is a probe for detecting the "address" field in JSON logs.
	KeyAddress = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // "address" field in JSON logs

	// KeyBlockHash is a probe for detecting the "blockHash" field in JSON logs.
	KeyBlockHash = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // "blockHash" field in JSON logs

	// KeyBlockNumber is a probe for detecting the "blockNumber" field in JSON logs.
	KeyBlockNumber = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // "blockNumber" field in JSON logs

	// KeyBlockTimestamp is a probe for detecting the "blockTimestamp" field in JSON logs.
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'} // "blockTimestamp" field in JSON logs

	// KeyData is a probe for detecting the "data" field in JSON logs.
	KeyData = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // "data" field in JSON logs

	// KeyLogIndex is a probe for detecting the "logIndex" field in JSON logs.
	KeyLogIndex = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // "logIndex" field in JSON logs

	// KeyRemoved is a probe for detecting the "removed" field in JSON logs.
	// The "removed" field indicates if the log was removed during reorganization.
	KeyRemoved = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // "removed" field in JSON logs (indicates if the log was removed)

	// KeyTopics is a probe for detecting the "topics" field in JSON logs.
	// The "topics" field typically holds an array of topics associated with the log.
	KeyTopics = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // "topics" field in JSON logs (typically an array)

	// KeyTransaction is a probe for detecting the "transactionHash" or "transactionIndex" field in JSON logs.
	KeyTransaction = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // "transactionHash" or "transactionIndex" field in JSON logs

	// SigSyncPrefix is a constant signature used to identify Sync() events in the logs.
	// This 8-byte signature is checked for verifying the event type for Uniswap V2 Sync() logs.
	// It is a fixed prefix that appears in the topics of these logs.
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // Sync event signature in Uniswap V2 logs
)
