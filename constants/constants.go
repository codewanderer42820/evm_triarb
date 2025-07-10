// constants.go — System-wide constants for high-performance arbitrage detection
package constants

// Core system limits
const (
	MaxSupportedCores   = 64      // Maximum CPU cores for parallelization
	DefaultRingSize     = 1 << 16 // 65,536 entries for inter-core buffers
	DefaultLocalIdxSize = 1 << 16 // 65,536 entries for local indexing
	MaxCyclesPerShard   = 1 << 16 // 65,536 cycles per shard for cache locality
)

// Memory management limits
const (
	HeapSoftLimit = 128 << 20 // 128 MiB - triggers non-blocking GC
	HeapHardLimit = 512 << 20 // 512 MiB - triggers panic on breach
	RingBits      = 18        // 2^18 = 262,144 dedup cache slots
	MaxReorg      = 128       // Maximum blockchain reorg depth
)

// Hash table configuration
const (
	AddressTableCapacity     = 1 << 20 // 1M entries for addresses
	AddressTableMask         = AddressTableCapacity - 1
	PairRoutingTableCapacity = 1 << 20 // 1M entries for routing
	AddressHexStart          = 2       // Address parsing start
	AddressHexEnd            = 42      // Address parsing end
)

// Arbitrage tick quantization
const (
	TickClampingBound         = 128.0   // Tick domain bounds [-128, +128]
	MaxQuantizedTick          = 262_143 // 18-bit ceiling (2^18 - 1)
	QuantizationScale         = (MaxQuantizedTick - 1) / (2 * TickClampingBound)
	MaxInitializationPriority = MaxQuantizedTick // Lowest priority for new cycles
)

// WebSocket configuration
const (
	WsDialAddr   = "polygon-mainnet.infura.io:443"
	WsPath       = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"
	WsHost       = "polygon-mainnet.infura.io"
	MaxFrameSize = 512 << 10 // 512 KiB frame size
	FrameCap     = 1 << 17   // 131,072 frames
)

// System initialization
const (
	ShardChannelBufferSize = 1 << 10 // 1,024 entries for shard distribution
)

// JSON parsing probes - 8-byte aligned for SIMD operations
var (
	// Core log fields
	KeyAddress = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // "address
	KeyData    = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // "data":"
	KeyTopics  = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // "topics"

	// Block metadata
	KeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // "blockHa
	KeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // "blockNu
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'} // "blockTi

	// Transaction fields
	KeyLogIndex    = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // "logInde
	KeyTransaction = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // "transac
	KeyRemoved     = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // "removed

	// Uniswap V2 Sync event signature
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // keccak256("Sync(uint112,uint112)")
)
