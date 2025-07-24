// ════════════════════════════════════════════════════════════════════════════════════════════════
// Historical Synchronization Harvester
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: CSV Reserve Harvester
//
// Description:
//   Historical data extraction system for building complete Uniswap V2 reserve state.
//   Uses optimized parsing with zero-copy operations for high-performance data processing.
//
// Features:
//   - Zero-allocation data processing with pre-allocated buffers
//   - SIMD-optimized hex parsing operations for maximum throughput
//   - Dynamic batch sizing with convergence for optimal performance
//   - Multi-connection parallel harvesting with intelligent load balancing
//   - Bit shift optimizations for all hot paths
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"main/constants"
	"main/debug"
	"main/router"
	"main/types"
	"main/utils"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sugawarayuuta/sonnet"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE DATA STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// EthereumLog represents a single log entry from Ethereum JSON-RPC responses.
//
//go:notinheap
//go:align 64
type EthereumLog struct {
	Data        string   // 16B - Hex-encoded event data payload (accessed first during processing)
	Address     string   // 16B - Contract address for the log event (accessed for pair lookup)
	BlockNumber string   // 16B - Block number in hex format (accessed for deduplication)
	_           [16]byte // 16B - Cache line padding
}

// EthereumLogsResponse encapsulates JSON-RPC log query responses.
//
//go:notinheap
//go:align 64
type EthereumLogsResponse struct {
	Result []EthereumLog // 24B - Array of log entries matching query criteria (hot path)
	Error  *struct {     // 8B - Error information from RPC provider (cold path)
		Message string // Error message if request failed
	}
	_ [32]byte // 32B - Cache line padding
}

// EthereumBlockResponse handles block number queries from JSON-RPC.
//
//go:notinheap
//go:align 64
type EthereumBlockResponse struct {
	Result string   // 16B - Current block number in hex format
	_      [48]byte // 48B - Cache line padding
}

// ProcessedReserveEntry represents a single reserve state after parsing.
//
//go:notinheap
//go:align 64
type ProcessedReserveEntry struct {
	contractAddress string   // 16B - Uniswap V2 pair contract address (used first for router lookup)
	eventData       string   // 16B - Raw hex data from Sync event (used second for reserve parsing)
	blockHeight     string   // 16B - Block number in hex format (used third for deduplication)
	_               [16]byte // 16B - Cache line padding
}

// SynchronizationHarvester orchestrates historical data extraction.
//
//go:notinheap
//go:align 64
type SynchronizationHarvester struct {
	// Cache Line 1: Nuclear hot - accessed during every operation (64B)
	totalEvents int64      // 8B - Atomic counter for processed events (read every batch)
	rpcEndpoint string     // 16B - RPC endpoint URL for HTTP requests (used every request)
	outputFile  *os.File   // 8B - CSV output file handle (written every flush)
	fileMutex   sync.Mutex // 8B - Protects concurrent file writes (locked every flush)
	batchSizes  []uint64   // 24B - Dynamic batch sizes per connection (read every batch)

	// Cache Line 2: Very hot - accessed frequently during processing (64B)
	csvBufferSizes       []int  // 24B - Current buffer sizes for CSV data (updated every record)
	consecutiveSuccesses []int  // 24B - Success counters for batch adaptation (updated every success)
	outputPath           string // 16B - Output file path for this harvester instance

	// Cache Line 3: Hot - accessed during batch operations (64B)
	currentBlocks []uint64       // 24B - Current block heights per connection (updated every batch)
	httpClients   []*http.Client // 24B - HTTP clients for parallel requests (used every request)
	_             [16]byte       // 16B - Cache line padding

	// Cache Line 4: Warm - accessed during coordination (64B)
	syncTarget        uint64             // 8B - Target block height for synchronization
	lastProcessed     uint64             // 8B - Last successfully processed block
	startTime         time.Time          // 24B - Processing start timestamp for reporting
	processingContext context.Context    // 8B - Context for cancellation handling
	cancelFunc        context.CancelFunc // 8B - Function to cancel processing context
	_                 [8]byte            // 8B - Cache line padding
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL PROCESSING BUFFERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Global buffers eliminate allocation overhead during processing operations.
//
//go:notinheap
//go:align 64
var (
	// Cache Line 1: Ultra hot - accessed during every HTTP request (64B)
	responseBuffers [][]byte                // 24B - HTTP response buffers (read/written every request)
	processedLogs   []ProcessedReserveEntry // 24B - Log processing buffer (accessed every batch)
	_               [16]byte                // 16B - Cache line padding

	// Cache Line 2: Hot - accessed during CSV operations (64B)
	csvOutputBuffers  [][]byte          // 24B - CSV output buffers for batched writes (written every record)
	csvStringBuilders []strings.Builder // 24B - String builders for zero-allocation CSV construction
	_                 [16]byte          // 16B - Cache line padding
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// METADATA MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// LoadMetadata reads the last processed block height from binary metadata file.
// Returns deployment block if metadata file is missing or corrupted.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LoadMetadata() uint64 {
	var buf [8]byte
	file, err := os.Open(constants.HarvesterMetadataPath)
	if err != nil {
		return constants.HarvesterDeploymentBlock
	}
	defer file.Close()

	if n, err := file.Read(buf[:]); err != nil || n != 8 {
		return constants.HarvesterDeploymentBlock
	}

	return utils.Load64(buf[:])
}

// saveMetadata writes the last processed block height to binary metadata file.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func saveMetadata(block uint64) error {
	buf := [8]byte{
		byte(block), byte(block >> 8), byte(block >> 16), byte(block >> 24),
		byte(block >> 32), byte(block >> 40), byte(block >> 48), byte(block >> 56),
	}

	file, err := os.OpenFile(constants.HarvesterMetadataPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(buf[:])
	return err
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HTTP TRANSPORT OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// buildHTTPTransport creates an optimized HTTP transport for maximum throughput operations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func buildHTTPTransport() *http.Transport {
	return &http.Transport{
		// Connection establishment (ultra hot - every new connection)
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,  // Connection establishment timeout
			KeepAlive: 60 * time.Second, // Connection keep-alive duration
			DualStack: true,             // Enable IPv4/IPv6 fallback support
		}).DialContext,

		// Connection pooling (very hot - accessed during every request)
		MaxIdleConns:        800, // Global connection pool size
		MaxIdleConnsPerHost: 150, // Per-host connection limit
		MaxConnsPerHost:     300, // Maximum concurrent connections per host

		// Timeout configuration (hot - checked during request lifecycle)
		IdleConnTimeout:       120 * time.Second,      // Idle connection timeout
		TLSHandshakeTimeout:   4 * time.Second,        // TLS negotiation timeout
		ResponseHeaderTimeout: 12 * time.Second,       // Response header timeout
		ExpectContinueTimeout: 500 * time.Millisecond, // Continue expectation timeout

		// Performance flags
		DisableCompression: true,  // Disable compression for speed
		DisableKeepAlives:  false, // Enable connection reuse
		ForceAttemptHTTP2:  true,  // Use HTTP/2 when available

		// Buffer configuration
		WriteBufferSize: 128 * 1024, // Write buffer size
		ReadBufferSize:  128 * 1024, // Read buffer size

		// Environment configuration
		Proxy: http.ProxyFromEnvironment,
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SIMD-OPTIMIZED HEX PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// parseReservesToZeroTrimmed extracts and trims reserve values from Sync event data.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func parseReservesToZeroTrimmed(eventData string) (string, string) {
	// Convert string to byte slice for SIMD processing efficiency
	dataBytes := unsafe.Slice(unsafe.StringData(eventData), len(eventData))

	// Parse reserve0 from bytes 32-64
	leadingZeros0 := utils.CountHexLeadingZeros(dataBytes[32:64])
	if leadingZeros0 == 32 {
		leadingZeros0 = 31 // Keep one zero for "0" representation
	}
	reserve0Start := 32 + leadingZeros0
	reserve0 := eventData[reserve0Start:64]

	// Parse reserve1 from bytes 96-128
	leadingZeros1 := utils.CountHexLeadingZeros(dataBytes[96:128])
	if leadingZeros1 == 32 {
		leadingZeros1 = 31 // Keep one zero for "0" representation
	}
	reserve1Start := 96 + leadingZeros1
	reserve1 := eventData[reserve1Start:128]

	return reserve0, reserve1
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTER INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// newSynchronizationHarvester creates a fully initialized harvester with optimized buffers and transport.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newSynchronizationHarvester(connectionCount int, outputPath string) *SynchronizationHarvester {
	ctx, cancel := context.WithCancel(context.Background())
	lastProcessed := LoadMetadata()

	harvester := &SynchronizationHarvester{
		// Cache Line 1: Nuclear hot fields
		totalEvents: 0,                                                              // 8B - Incremented constantly
		rpcEndpoint: "https://" + constants.HarvesterHost + constants.HarvesterPath, // 16B - Used in every HTTP request
		outputFile:  nil,                                                            // 8B - Set below, used in every flush
		fileMutex:   sync.Mutex{},                                                   // 8B - Locked during every flush operation
		batchSizes:  make([]uint64, connectionCount),                                // 24B - Read during every batch operation

		// Cache Line 2: Very hot fields
		csvBufferSizes:       make([]int, connectionCount), // 24B - Updated with every CSV record
		consecutiveSuccesses: make([]int, connectionCount), // 24B - Updated after every successful batch
		outputPath:           outputPath,                   // 16B - Used during initialization

		// Cache Line 3: Hot fields
		currentBlocks: make([]uint64, connectionCount),       // 24B - Updated once per batch
		httpClients:   make([]*http.Client, connectionCount), // 24B - Used for every HTTP request

		// Cache Line 4: Warm fields
		syncTarget:        0,             // 8B - Set once at start, read at end
		lastProcessed:     lastProcessed, // 8B - Read at start for resumption logic
		startTime:         time.Now(),    // 24B - Set once, read periodically for reporting
		processingContext: ctx,           // 8B - Checked periodically for cancellation
		cancelFunc:        cancel,        // 8B - Called once during cleanup
	}

	// Initialize batch sizes to optimal defaults
	for i := range harvester.batchSizes {
		harvester.batchSizes[i] = constants.OptimalBatchSize
	}

	// Initialize global buffers
	responseBuffers = make([][]byte, connectionCount)
	processedLogs = make([]ProcessedReserveEntry, constants.MaxLogSliceSize)
	csvOutputBuffers = make([][]byte, connectionCount)
	csvStringBuilders = make([]strings.Builder, connectionCount)

	// Create shared HTTP transport for connection pooling efficiency
	sharedTransport := buildHTTPTransport()
	for i := 0; i < connectionCount; i++ {
		responseBuffers[i] = make([]byte, constants.ResponseBufferSize)
		csvOutputBuffers[i] = make([]byte, constants.CSVBufferSize)
		csvStringBuilders[i].Grow(constants.CSVBufferSize)
		harvester.httpClients[i] = &http.Client{
			Timeout:   30 * time.Second,
			Transport: sharedTransport,
		}
	}

	// File mode calculation based on resumption state
	var err error
	fileMode := os.O_CREATE | os.O_WRONLY
	if lastProcessed == constants.HarvesterDeploymentBlock {
		fileMode |= os.O_TRUNC // Start fresh
	} else {
		fileMode |= os.O_APPEND // Resume existing file
	}

	harvester.outputFile, err = os.OpenFile(outputPath, fileMode, 0644)
	if err != nil {
		panic(err)
	}

	// Write CSV header for new files only
	if lastProcessed == constants.HarvesterDeploymentBlock {
		harvester.outputFile.WriteString("address,block,reserve0,reserve1\n")
	}

	// Start background reporting goroutine
	go harvester.reportStatistics()
	return harvester
}

// reportStatistics provides periodic progress updates during harvesting operation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) reportStatistics() {
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			events := atomic.LoadInt64(&harvester.totalEvents)
			debug.DropMessage("HARVEST", utils.Itoa(int(events))+" events processed")

			for sectorID := 0; sectorID < len(harvester.currentBlocks); sectorID++ {
				currentBlock := harvester.currentBlocks[sectorID] // Racey read for best-effort reporting
				if currentBlock > 0 {
					debug.DropMessage("HARVEST", "Sector "+utils.Itoa(sectorID)+" at block "+utils.Itoa(int(currentBlock)))
				}
			}

		case <-harvester.processingContext.Done():
			return
		}
	}
}

// cleanup ensures all resources are properly released and data is flushed.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) cleanup() {
	// Data safety first
	harvester.flushAllBuffers()

	// File cleanup
	if harvester.outputFile != nil {
		harvester.outputFile.Close()
	}

	// Memory cleanup
	responseBuffers = nil
	processedLogs = nil
	csvOutputBuffers = nil
	csvStringBuilders = nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ETHEREUM JSON-RPC OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// getCurrentBlockNumber queries the latest block number from the Ethereum node.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) getCurrentBlockNumber() uint64 {
	requestJSON := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(harvester.processingContext, 8*time.Second)
		req, err := http.NewRequestWithContext(ctx, "POST", harvester.rpcEndpoint, strings.NewReader(requestJSON))
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		response, err := harvester.httpClients[0].Do(req)
		if err != nil {
			cancel()
			time.Sleep(25 * time.Millisecond)
			continue
		}

		bytesRead, _ := response.Body.Read(responseBuffers[0][:512])
		response.Body.Close()
		cancel()

		if bytesRead == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		var blockResponse EthereumBlockResponse
		err = sonnet.Unmarshal(responseBuffers[0][:bytesRead], &blockResponse)
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		// Validate response format for network resilience
		if len(blockResponse.Result) > 2 {
			blockNumber := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
			return blockNumber
		}

		time.Sleep(5 * time.Millisecond)
	}

	panic("Failed to get current block number after retries")
}

// extractLogBatch retrieves and processes logs for a specific block range.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) extractLogBatch(fromBlock, toBlock uint64, connectionID int) (int, error) {
	builder := &csvStringBuilders[connectionID]
	builder.Reset()

	fromHex := fmt.Sprintf("%x", fromBlock)
	toHex := fmt.Sprintf("%x", toBlock)

	// Construct JSON-RPC request manually for optimal performance
	builder.WriteString(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x`)
	builder.WriteString(fromHex)
	builder.WriteString(`","toBlock":"0x`)
	builder.WriteString(toHex)
	builder.WriteString(`","topics":["`)
	builder.WriteString(constants.SyncEventSignature)
	builder.WriteString(`"]}],"id":1}`)

	requestJSON := builder.String()

	ctx, cancel := context.WithTimeout(harvester.processingContext, 25*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", harvester.rpcEndpoint, strings.NewReader(requestJSON))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	response, err := harvester.httpClients[connectionID].Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d", response.StatusCode)
	}

	// Response reading with cache-aligned operations
	totalBytes := 0
	responseBuffer := responseBuffers[connectionID]
	maxReadSize := (len(responseBuffer) - 1024) &^ 63 // Cache line alignment

	for totalBytes < maxReadSize {
		readSize := constants.ReadBufferSize &^ 15 // 16-byte alignment
		if totalBytes+readSize > maxReadSize {
			readSize = (maxReadSize - totalBytes) &^ 15
		}

		bytesRead, err := response.Body.Read(responseBuffer[totalBytes : totalBytes+readSize])
		totalBytes += bytesRead

		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return 0, err
		}

		if bytesRead == 0 {
			break
		}
	}

	if totalBytes == 0 {
		return 0, fmt.Errorf("empty response") // Network resilience check
	}

	return harvester.parseLogsWithSonnet(responseBuffer[:totalBytes], connectionID)
}

// parseLogsWithSonnet processes JSON-RPC log responses using optimized parsing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) parseLogsWithSonnet(jsonData []byte, connectionID int) (int, error) {
	connectionCount := len(harvester.httpClients)
	bufferOffset := connectionID * (len(processedLogs) / connectionCount)
	maxLogsPerConnection := len(processedLogs) / connectionCount
	logCount := 0

	var logsResponse EthereumLogsResponse
	err := sonnet.Unmarshal(jsonData, &logsResponse)
	if err != nil {
		return 0, err
	}

	if logsResponse.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", logsResponse.Error.Message)
	}

	for _, ethereumLog := range logsResponse.Result {
		if logCount >= maxLogsPerConnection {
			break // Prevent buffer overflow
		}

		bufferPosition := bufferOffset + logCount

		logEntry := &processedLogs[bufferPosition]
		logEntry.contractAddress = ethereumLog.Address[2:] // Remove 0x prefix
		logEntry.eventData = ethereumLog.Data[2:]          // Remove 0x prefix
		logEntry.blockHeight = ethereumLog.BlockNumber[2:] // Remove 0x prefix

		logCount++
	}

	return logCount, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CSV OUTPUT OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// writeCSVRecord constructs and buffers a CSV record for batch writing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) writeCSVRecord(address, blockHeight, reserve0, reserve1 string, connectionID int) {
	builder := &csvStringBuilders[connectionID]
	builder.Reset()

	builder.WriteString(address)
	builder.WriteByte(',')
	builder.WriteString(blockHeight)
	builder.WriteByte(',')
	builder.WriteString(reserve0)
	builder.WriteByte(',')
	builder.WriteString(reserve1)
	builder.WriteByte('\n')

	csvRecord := builder.String()

	currentBufferSize := harvester.csvBufferSizes[connectionID]
	newBufferSize := currentBufferSize + len(csvRecord)

	threshold := constants.CSVBufferSize - (constants.CSVBufferSize >> 3) // 87.5% threshold
	if newBufferSize >= threshold {
		harvester.flushCSVBuffer(connectionID)
		currentBufferSize = 0
		newBufferSize = len(csvRecord)
	}

	copy(csvOutputBuffers[connectionID][currentBufferSize:], csvRecord)
	harvester.csvBufferSizes[connectionID] = newBufferSize
	atomic.AddInt64(&harvester.totalEvents, 1)
}

// flushCSVBuffer writes accumulated CSV data to disk with mutex protection.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) flushCSVBuffer(connectionID int) {
	if harvester.csvBufferSizes[connectionID] == 0 {
		return
	}

	harvester.fileMutex.Lock()
	harvester.outputFile.Write(csvOutputBuffers[connectionID][:harvester.csvBufferSizes[connectionID]])
	harvester.fileMutex.Unlock()

	harvester.csvBufferSizes[connectionID] = 0
}

// flushAllBuffers ensures all pending CSV data is written to disk and synced.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) flushAllBuffers() {
	for i := 0; i < len(harvester.httpClients); i++ {
		harvester.flushCSVBuffer(i)
	}
	harvester.outputFile.Sync()
}

// processLogFromGlobalBuffer converts parsed log data into CSV format and tracks block progress.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) processLogFromGlobalBuffer(logEntry *ProcessedReserveEntry, connectionID int) {
	reserve0, reserve1 := parseReservesToZeroTrimmed(logEntry.eventData)

	harvester.writeCSVRecord(logEntry.contractAddress, logEntry.blockHeight, reserve0, reserve1, connectionID)

	blockNumber := utils.ParseHexU64([]byte(logEntry.blockHeight))
	if blockNumber > harvester.currentBlocks[connectionID] {
		harvester.currentBlocks[connectionID] = blockNumber
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTING COORDINATION ENGINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// executeHarvesting orchestrates the complete harvesting process with intelligent work distribution.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) executeHarvesting() error {
	harvester.syncTarget = harvester.getCurrentBlockNumber()

	if harvester.lastProcessed >= harvester.syncTarget {
		debug.DropMessage("HARVEST", "Already synced to latest block")
		return nil
	}

	totalBlocks := harvester.syncTarget - harvester.lastProcessed
	connectionCount := len(harvester.httpClients)
	blocksPerSector := totalBlocks / uint64(connectionCount)
	extraBlocks := totalBlocks % uint64(connectionCount)
	sectorStart := harvester.lastProcessed + 1

	debug.DropMessage("HARVEST", fmt.Sprintf("Resuming from block %d, %d blocks remaining, %d connections",
		harvester.lastProcessed, totalBlocks, connectionCount))

	// Calculate work distribution across connections with load balancing
	workSectors := make([][2]uint64, connectionCount)
	for i := 0; i < connectionCount; i++ {
		fromBlock := sectorStart
		sectorSize := blocksPerSector
		if uint64(i) < extraBlocks {
			sectorSize++
		}
		toBlock := fromBlock + sectorSize - 1
		if i == connectionCount-1 {
			toBlock = harvester.syncTarget // Ensure last sector reaches target
		}

		workSectors[i] = [2]uint64{fromBlock, toBlock}
		sectorStart = toBlock + 1
	}

	// Start periodic buffer flush goroutine for data safety
	flushTicker := time.NewTicker(2500 * time.Millisecond)
	go func() {
		defer flushTicker.Stop()
		for {
			select {
			case <-flushTicker.C:
				harvester.flushAllBuffers()
			case <-harvester.processingContext.Done():
				return
			}
		}
	}()

	// Launch all worker goroutines with proper synchronization
	var workerWG sync.WaitGroup
	for connectionID := 0; connectionID < connectionCount; connectionID++ {
		workerWG.Add(1)
		go func(id int, sectorRange [2]uint64) {
			defer workerWG.Done()
			harvester.harvestSector(sectorRange[0], sectorRange[1], id)
		}(connectionID, workSectors[connectionID])
	}

	workerWG.Wait()

	harvester.cancelFunc()
	harvester.flushAllBuffers()

	events := atomic.LoadInt64(&harvester.totalEvents)
	debug.DropMessage("HARVEST", utils.Itoa(int(events))+" events complete")

	return nil
}

// harvestSector processes a continuous range of blocks for a specific connection.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) harvestSector(fromBlock, toBlock uint64, connectionID int) {
	bufferOffset := connectionID * (len(processedLogs) / len(harvester.httpClients))
	currentBlock := fromBlock

	for currentBlock <= toBlock {
		batchSize := harvester.batchSizes[connectionID]
		batchEnd := currentBlock + batchSize - 1
		if batchEnd > toBlock {
			batchEnd = toBlock
		}

		logCount, err := harvester.extractLogBatch(currentBlock, batchEnd, connectionID)
		if err != nil {
			// Handle "too many results" error by reducing batch size
			if strings.Contains(err.Error(), "more than 10000 results") {
				harvester.batchSizes[connectionID] = harvester.batchSizes[connectionID] >> 1
				if harvester.batchSizes[connectionID] < constants.MinBatchSize {
					harvester.batchSizes[connectionID] = constants.MinBatchSize
				}
				harvester.consecutiveSuccesses[connectionID] = 0
				continue
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Process retrieved logs using pre-calculated buffer offset
		for i := 0; i < logCount; i++ {
			logPosition := bufferOffset + i
			if logPosition >= len(processedLogs) {
				break
			}
			harvester.processLogFromGlobalBuffer(&processedLogs[logPosition], connectionID)
		}

		currentBlock = batchEnd + 1
		harvester.consecutiveSuccesses[connectionID]++

		// Increase batch size after consecutive successes for adaptive optimization
		if harvester.consecutiveSuccesses[connectionID] >= 3 {
			harvester.batchSizes[connectionID] = harvester.batchSizes[connectionID] << 1
			harvester.consecutiveSuccesses[connectionID] = 0
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROUTER STATE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processLine parses a single CSV line and updates the router with optimized zero-copy operations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processLine(line []byte, processed *int, offset int64, blocks []uint64, addressBuf, dataBuf []byte, v *types.LogView) {
	// CSV delimiter discovery using linear scan
	c1, c2, c3 := -1, -1, -1
	for i, b := range line {
		if b == ',' {
			if c1 == -1 {
				c1 = i // First comma: separates address from block
			} else if c2 == -1 {
				c2 = i // Second comma: separates block from reserve0
			} else if c3 == -1 {
				c3 = i // Third comma: separates reserve0 from reserve1
				break
			}
		}
	}

	if c1 == 7 {
		return // Skip header row
	}

	pairID := router.LookupPairByAddress(line[0:c1])
	if pairID == 0 {
		return // Skip unknown pairs
	}

	block := utils.ParseHexU64(line[c1+1 : c2])
	if block <= blocks[pairID] {
		return // Skip stale blocks
	}
	blocks[pairID] = block

	// Address buffer preparation using SIMD-style operations
	addrWords := (*[5]uint64)(unsafe.Pointer(&addressBuf[2]))
	addrWords[0] = utils.Load64(line[0:8])   // Characters 0-7
	addrWords[1] = utils.Load64(line[8:16])  // Characters 8-15
	addrWords[2] = utils.Load64(line[16:24]) // Characters 16-23
	addrWords[3] = utils.Load64(line[24:32]) // Characters 24-31
	addrWords[4] = utils.Load64(line[32:40]) // Characters 32-39

	// Reserve data buffer initialization with pattern fills
	r0 := (*[4]uint64)(unsafe.Pointer(&dataBuf[34])) // Reserve0 data region
	r1 := (*[4]uint64)(unsafe.Pointer(&dataBuf[98])) // Reserve1 data region
	for i := 0; i < 4; i++ {
		r0[i] = 0x3030303030303030 // Eight ASCII '0' characters per 64-bit word
		r1[i] = 0x3030303030303030 // Ensures consistent zero-padding format
	}

	res0 := line[c2+1 : c3] // Reserve0 field from CSV
	res1 := line[c3+1:]     // Reserve1 field from CSV

	// Right-aligned hex placement for standardized formatting
	copy(dataBuf[66-len(res0):66], res0)   // Right-align reserve0 in its 32-char field
	copy(dataBuf[130-len(res1):130], res1) // Right-align reserve1 in its 32-char field

	router.DispatchPriceUpdate(v)
	*processed++
}

// flushHarvestedReservesToRouterFromFile performs backwards streaming CSV ingestion for router state initialization.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func flushHarvestedReservesToRouterFromFile(filePath string) error {
	db, err := sql.Open("sqlite3", "uniswap_pairs.db")
	if err != nil {
		return err
	}
	defer db.Close()

	var totalPairs int
	if err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&totalPairs); err != nil {
		return err
	}
	if totalPairs == 0 {
		return fmt.Errorf("no pairs found in database")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	var logView types.LogView

	addressBuffer := make([]byte, 42)
	addressBuffer[0], addressBuffer[1] = '0', 'x' // Ethereum address prefix

	dataBuffer := make([]byte, 130)
	dataBuffer[0], dataBuffer[1] = '0', 'x' // Event data prefix

	// Reserve data pre-initialization using 64-bit pattern fills
	zeros1 := (*[4]uint64)(unsafe.Pointer(&dataBuffer[34])) // Reserve0 data region
	zeros2 := (*[4]uint64)(unsafe.Pointer(&dataBuffer[98])) // Reserve1 data region
	for i := 0; i < 4; i++ {
		zeros1[i] = 0x3030303030303030 // Eight ASCII '0' characters per 64-bit word
		zeros2[i] = 0x3030303030303030 // Ensures consistent zero-padding format
	}

	logView.Addr = addressBuffer
	logView.Data = dataBuffer

	blockHeights := make([]uint64, totalPairs+1)
	processedEvents := 0

	// Backwards file reading infrastructure
	const bufferSize = 8192
	readBuffer := make([]byte, bufferSize)
	workingBuffer := make([]byte, bufferSize<<1)
	lineBuffer := make([]byte, bufferSize)
	lineBufferUsed := 0
	currentPosition := stat.Size()

	// Backwards streaming loop with chunk-based processing
	for currentPosition > 0 {
		readSize := bufferSize
		if currentPosition < bufferSize {
			readSize = int(currentPosition)
		}
		currentPosition -= int64(readSize)

		_, err := file.ReadAt(readBuffer[:readSize], currentPosition)
		if err != nil {
			return err
		}

		var processingData []byte
		if lineBufferUsed > 0 {
			copy(workingBuffer, readBuffer[:readSize])
			copy(workingBuffer[readSize:], lineBuffer[:lineBufferUsed])
			processingData = workingBuffer[:readSize+lineBufferUsed]
			lineBufferUsed = 0
		} else {
			processingData = readBuffer[:readSize]
		}

		// Backwards line extraction with newline boundary detection
		endPosition := len(processingData)
		for scanIndex := len(processingData) - 1; scanIndex >= 0; scanIndex-- {
			if processingData[scanIndex] == '\n' {
				if scanIndex+1 < endPosition {
					lineData := processingData[scanIndex+1 : endPosition]
					if len(lineData) > 0 {
						fileOffset := currentPosition + int64(scanIndex+1)
						processLine(lineData, &processedEvents, fileOffset, blockHeights, addressBuffer, dataBuffer, &logView)
					}
				}
				endPosition = scanIndex
			}
		}

		if endPosition > 0 {
			copy(lineBuffer, processingData[:endPosition])
			lineBufferUsed = endPosition
		}
	}

	if lineBufferUsed > 0 {
		processLine(lineBuffer[:lineBufferUsed], &processedEvents, 0, blockHeights, addressBuffer, dataBuffer, &logView)
	}

	debug.DropMessage("HARVEST", utils.Itoa(processedEvents)+" reserve states loaded into router")
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// CheckHarvestingRequirement determines if harvesting is needed by comparing block heights.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func CheckHarvestingRequirement() (bool, uint64, uint64, error) {
	client := &http.Client{Timeout: 10 * time.Second, Transport: buildHTTPTransport()}
	rpcEndpoint := "https://" + constants.HarvesterHost + constants.HarvesterPath

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", rpcEndpoint, strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	if err != nil {
		return false, 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	response, err := client.Do(req)
	if err != nil {
		return false, 0, 0, err
	}
	defer response.Body.Close()

	var buf [128]byte
	n, _ := response.Body.Read(buf[:])

	var blockResponse EthereumBlockResponse
	if err := sonnet.Unmarshal(buf[:n], &blockResponse); err != nil {
		return false, 0, 0, err
	}

	// Validate response format for network resilience
	if len(blockResponse.Result) > 2 {
		currentHeight := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
		lastProcessed := LoadMetadata()
		return lastProcessed < currentHeight, lastProcessed, currentHeight, nil
	}

	return false, 0, 0, fmt.Errorf("invalid response format")
}

// CheckHarvestingRequirementFromBlock determines if harvesting is needed using provided last block.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func CheckHarvestingRequirementFromBlock(lastProcessedBlock uint64) (bool, uint64, uint64, error) {
	client := &http.Client{Timeout: 10 * time.Second, Transport: buildHTTPTransport()}
	rpcEndpoint := "https://" + constants.HarvesterHost + constants.HarvesterPath

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", rpcEndpoint, strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	if err != nil {
		return false, 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	response, err := client.Do(req)
	if err != nil {
		return false, 0, 0, err
	}
	defer response.Body.Close()

	var buf [128]byte
	n, _ := response.Body.Read(buf[:])

	var blockResponse EthereumBlockResponse
	if err := sonnet.Unmarshal(buf[:n], &blockResponse); err != nil {
		return false, 0, 0, err
	}

	// Validate response format for network resilience
	if len(blockResponse.Result) > 2 {
		currentHeight := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
		return lastProcessedBlock < currentHeight, lastProcessedBlock, currentHeight, nil
	}

	return false, 0, 0, fmt.Errorf("invalid response format")
}

// ExecuteHarvesting starts the harvesting process with default connection settings.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ExecuteHarvesting() error {
	return ExecuteHarvestingWithConnections(constants.DefaultConnections)
}

// ExecuteHarvestingWithConnections starts the harvesting process with specified connection count.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func ExecuteHarvestingWithConnections(connectionCount int) error {
	harvester := newSynchronizationHarvester(connectionCount, constants.HarvesterOutputPath)
	defer harvester.cleanup()

	err := harvester.executeHarvesting()
	if err == nil {
		saveMetadata(harvester.syncTarget) // Save metadata only after successful harvesting
	}
	return err
}

// ExecuteHarvestingToTemp performs temp harvesting and returns the last processed block.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func ExecuteHarvestingToTemp(connectionCount int) (uint64, error) {
	harvester := newSynchronizationHarvester(connectionCount, constants.HarvesterTempPath)
	defer harvester.cleanup()

	err := harvester.executeHarvesting()
	if err != nil {
		return 0, err
	}

	return harvester.syncTarget, nil
}

// FlushHarvestedReservesToRouter performs backwards streaming CSV ingestion for router state initialization.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func FlushHarvestedReservesToRouter() error {
	return flushHarvestedReservesToRouterFromFile(constants.HarvesterOutputPath)
}

// FlushHarvestedReservesToRouterFromTemp performs the same operation but reads from the temp file.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func FlushHarvestedReservesToRouterFromTemp() error {
	return flushHarvestedReservesToRouterFromFile(constants.HarvesterTempPath)
}
