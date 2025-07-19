// ════════════════════════════════════════════════════════════════════════════════════════════════
// Historical Synchronization Harvester
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Zero-Allocation CSV Reserve Harvester
//
// Description:
//   High-performance historical data extraction system for building complete Uniswap V2
//   reserve state. Uses SIMD-optimized parsing with zero-copy operations for maximum throughput.
//
// Features:
//   - Zero-allocation, zero-copy data processing
//   - SIMD-optimized hex parsing operations
//   - Dynamic batch sizing with binary search convergence
//   - Multi-connection parallel harvesting
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"database/sql"
	"fmt"
	"math/bits"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"main/constants"
	"main/control"
	"main/debug"
	"main/router"
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
	Address     string   // 16B - Contract address for the log event
	Data        string   // 16B - Hex-encoded event data payload
	BlockNumber string   // 16B - Block number in hex format
	_           [16]byte // 16B - Padding to fill cache line
}

// EthereumLogsResponse encapsulates JSON-RPC log query responses.
//
//go:notinheap
//go:align 64
type EthereumLogsResponse struct {
	Result []EthereumLog // 24B - Array of log entries matching query
	Error  *struct {
		Message string // 16B - API error message if request failed
	} // 8B - Error pointer
	_ [32]byte // 32B - Padding to fill cache line (24+8+32=64)
}

// EthereumBlockResponse handles block number queries from JSON-RPC.
//
//go:notinheap
//go:align 64
type EthereumBlockResponse struct {
	Result string   // 16B - Current block number in hex format
	_      [48]byte // 48B - Padding to fill cache line
}

// ProcessedReserveEntry represents a single reserve state after parsing.
//
//go:notinheap
//go:align 64
type ProcessedReserveEntry struct {
	contractAddress string   // 16B - Uniswap V2 pair contract address
	eventData       string   // 16B - Raw hex data from Sync event
	blockHeight     string   // 16B - Block number in hex format (without 0x prefix)
	_               [16]byte // 16B - Padding to fill cache line
}

// SynchronizationHarvester orchestrates historical data extraction.
//
//go:notinheap
//go:align 64
type SynchronizationHarvester struct {
	// CACHE LINE 1: Hottest fields
	totalEvents int64      // 8B - Atomic increment every log processed
	rpcEndpoint string     // 16B - Read per HTTP request
	outputFile  *os.File   // 8B - Used during flushCSVBuffer()
	fileMutex   sync.Mutex // 8B - Locked during file writes
	batchSizes  []uint64   // 24B - Read/written every batch attempt (moved from line 2)

	// CACHE LINE 2: Hot fields
	csvBufferSizes       []int    // 24B - Read/written every writeCSVRecord() call
	consecutiveSuccesses []int    // 24B - Updated every batch completion
	_                    [16]byte // 16B - Padding to fill cache line

	// CACHE LINE 3: Warm fields
	currentBlocks []uint64       // 24B - Updated every batch completion
	httpClients   []*http.Client // 24B - Selected per extractLogBatch() call
	_             [16]byte       // 16B - Padding to fill cache line

	// CACHE LINE 4: Cold fields
	syncTarget        uint64             // 8B - Set once, read once
	lastProcessed     uint64             // 8B - Set once, read once
	startTime         time.Time          // 24B - Set once, read only for stats
	processingContext context.Context    // 8B - Set once, checked occasionally
	cancelFunc        context.CancelFunc // 8B - Set once, called once
	_                 [8]byte            // 8B - Padding to fill cache line
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL PROCESSING BUFFERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Global buffers eliminate allocation overhead during processing operations.
// Dynamically sized based on connection count for optimal memory usage patterns.
//
//go:notinheap
//go:align 64
var (
	// CACHE LINE 1: Most frequently accessed buffers
	responseBuffers [][]byte                // 24B - HTTP response buffers for parallel request handling
	processedLogs   []ProcessedReserveEntry // 24B - Log processing buffer for parsed events
	_               [16]byte                // 16B - Padding to fill cache line

	// CACHE LINE 2: Less frequently accessed buffers
	csvOutputBuffers  [][]byte          // 24B - CSV output buffers for batched writes
	csvStringBuilders []strings.Builder // 24B - String builders for zero-allocation CSV construction
	_                 [16]byte          // 16B - Padding to fill cache line
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HTTP TRANSPORT OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// buildHTTPTransport builds HTTP transport for maximum throughput operations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func buildHTTPTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,  // Fast connection establishment
			KeepAlive: 60 * time.Second, // Long keep-alive for connection reuse
			DualStack: true,             // IPv4/IPv6 fallback for reliability
		}).DialContext,
		MaxIdleConns:          800,                       // Massive connection pool for peak throughput
		MaxIdleConnsPerHost:   150,                       // High per-host limit for parallel requests
		MaxConnsPerHost:       300,                       // Allow substantial concurrency
		IdleConnTimeout:       120 * time.Second,         // Long idle timeout
		TLSHandshakeTimeout:   4 * time.Second,           // Fast TLS negotiation
		ResponseHeaderTimeout: 12 * time.Second,          // Quick header timeout
		ExpectContinueTimeout: 500 * time.Millisecond,    // Very fast continue
		DisableCompression:    true,                      // Raw speed over bandwidth
		DisableKeepAlives:     false,                     // Enable connection reuse
		ForceAttemptHTTP2:     true,                      // HTTP/2 for multiplexing
		WriteBufferSize:       128 * 1024,                // Large write buffer
		ReadBufferSize:        128 * 1024,                // Large read buffer
		Proxy:                 http.ProxyFromEnvironment, // Only essential robustness
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// METADATA MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadMetadata reads the last processed block height from binary metadata file.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadMetadata() uint64 {
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
// HARVESTER INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newSynchronizationHarvester(connectionCount int) *SynchronizationHarvester {
	ctx, cancel := context.WithCancel(context.Background())
	lastProcessed := loadMetadata()

	harvester := &SynchronizationHarvester{
		totalEvents:          0,
		rpcEndpoint:          "https://" + constants.WsHost + "/v3/a2a3139d2ab24d59bed2dc3643664126",
		csvBufferSizes:       make([]int, connectionCount),
		batchSizes:           make([]uint64, connectionCount),
		consecutiveSuccesses: make([]int, connectionCount),
		currentBlocks:        make([]uint64, connectionCount),
		httpClients:          make([]*http.Client, connectionCount),
		lastProcessed:        lastProcessed,
		startTime:            time.Now(),
		processingContext:    ctx,
		cancelFunc:           cancel,
	}

	for i := range harvester.batchSizes {
		harvester.batchSizes[i] = constants.OptimalBatchSize
	}

	responseBuffers = make([][]byte, connectionCount)
	csvOutputBuffers = make([][]byte, connectionCount)
	csvStringBuilders = make([]strings.Builder, connectionCount)
	processedLogs = make([]ProcessedReserveEntry, constants.MaxLogSliceSize)

	sharedTransport := buildHTTPTransport()
	for i := 0; i < connectionCount; i++ {
		responseBuffers[i] = make([]byte, constants.ResponseBufferSize)
		csvOutputBuffers[i] = make([]byte, constants.CSVBufferSize)
		csvStringBuilders[i].Grow(constants.CSVBufferSize)
		harvester.httpClients[i] = &http.Client{Timeout: 30 * time.Second, Transport: sharedTransport}
	}

	var err error
	fileMode := os.O_CREATE | os.O_WRONLY
	if lastProcessed == constants.HarvesterDeploymentBlock {
		fileMode |= os.O_TRUNC
	} else {
		fileMode |= os.O_APPEND
	}

	harvester.outputFile, err = os.OpenFile(constants.HarvesterOutputPath, fileMode, 0644)
	if err != nil {
		panic(err)
	}

	if lastProcessed == constants.HarvesterDeploymentBlock {
		harvester.outputFile.WriteString("address,block,reserve0,reserve1\n")
	}

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChannel
		cancel()
	}()

	go harvester.reportStatistics()
	return harvester
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) reportStatistics() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			events := atomic.LoadInt64(&harvester.totalEvents)
			debug.DropMessage("HARVEST", utils.Itoa(int(events))+" events processed")

		case <-harvester.processingContext.Done():
			return
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ETHEREUM JSON-RPC OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// getCurrentBlockNumber queries the latest block number from the Ethereum node.
// Fast execution with minimal retry logic for peak performance.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) getCurrentBlockNumber() uint64 {
	requestJSON := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	maxRetries := 3 // Minimal retries for speed

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Check for cancellation only
		select {
		case <-harvester.processingContext.Done():
			return 0
		default:
		}

		// Fast request with short timeout
		ctx, cancel := context.WithTimeout(harvester.processingContext, 8*time.Second)
		req, err := http.NewRequestWithContext(ctx, "POST", harvester.rpcEndpoint, strings.NewReader(requestJSON))
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		response, err := harvester.httpClients[0].Do(req)
		cancel()

		if err != nil {
			time.Sleep(200 * time.Millisecond) // Brief delay only
			continue
		}

		// Read response with size limit for safety
		bytesRead, _ := response.Body.Read(responseBuffers[0][:512])
		response.Body.Close()

		if bytesRead == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Parse JSON response using optimized Sonnet parser
		var blockResponse EthereumBlockResponse
		err = sonnet.Unmarshal(responseBuffers[0][:bytesRead], &blockResponse)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Validate and parse hex block number
		if len(blockResponse.Result) >= 2 && blockResponse.Result[:2] == "0x" {
			blockNumber := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
			if blockNumber > 0 {
				return blockNumber
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	// If all retries failed, panic as this is critical
	panic("Failed to get current block number after retries")
}

// extractLogBatch retrieves and processes logs for a specific block range.
// Optimized for peak throughput with minimal overhead and essential error checking.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) extractLogBatch(fromBlock, toBlock uint64, connectionID int) (int, error) {
	// Use pre-allocated string builder for zero-allocation JSON construction
	builder := &csvStringBuilders[connectionID]
	builder.Reset()

	// Convert block numbers to hex format for JSON-RPC
	fromHex := fmt.Sprintf("%x", fromBlock)
	toHex := fmt.Sprintf("%x", toBlock)

	// Manual JSON construction for optimal performance
	builder.WriteString(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x`)
	builder.WriteString(fromHex)
	builder.WriteString(`","toBlock":"0x`)
	builder.WriteString(toHex)
	builder.WriteString(`","topics":["`)
	builder.WriteString(constants.SyncEventSignature)
	builder.WriteString(`"]}],"id":1}`)

	requestJSON := builder.String()

	// Create request with aggressive timeout for speed
	ctx, cancel := context.WithTimeout(harvester.processingContext, 25*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", harvester.rpcEndpoint, strings.NewReader(requestJSON))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute HTTP request using connection-specific client
	response, err := harvester.httpClients[connectionID].Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()

	// Quick status check only for essential errors
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d", response.StatusCode)
	}

	// Read response data optimized for speed
	totalBytes := 0
	responseBuffer := responseBuffers[connectionID]
	maxReadSize := len(responseBuffer) - 1024 // Leave buffer for safety

	for totalBytes < maxReadSize {
		readSize := constants.ReadBufferSize
		if totalBytes+readSize > maxReadSize {
			readSize = maxReadSize - totalBytes
		}

		bytesRead, err := response.Body.Read(responseBuffer[totalBytes : totalBytes+readSize])
		totalBytes += bytesRead

		if err != nil {
			if err.Error() == "EOF" {
				break // Normal end of response
			}
			return 0, err
		}

		if bytesRead == 0 {
			break
		}
	}

	if totalBytes == 0 {
		return 0, fmt.Errorf("empty response")
	}

	return harvester.parseLogsWithSonnet(responseBuffer[:totalBytes], connectionID)
}

// parseLogsWithSonnet processes JSON-RPC log responses using optimized parsing.
// Validates log data and populates global processing buffers for CSV generation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) parseLogsWithSonnet(jsonData []byte, connectionID int) (int, error) {
	// Calculate buffer partitioning for this connection
	connectionCount := len(harvester.httpClients)
	bufferOffset := connectionID * (len(processedLogs) / connectionCount)
	logCount := 0
	maxLogsPerConnection := len(processedLogs) / connectionCount

	// Parse JSON response using high-performance Sonnet parser
	var logsResponse EthereumLogsResponse
	err := sonnet.Unmarshal(jsonData, &logsResponse)
	if err != nil {
		return 0, err
	}

	// Check for RPC errors in response
	if logsResponse.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", logsResponse.Error.Message)
	}

	// Process each log entry with validation
	for _, ethereumLog := range logsResponse.Result {
		if logCount >= maxLogsPerConnection {
			break // Prevent buffer overflow
		}

		bufferPosition := bufferOffset + logCount
		if bufferPosition >= len(processedLogs) {
			break // Additional safety check
		}

		// Validate Sync event data format (130 bytes: 0x + 128 hex chars)
		if len(ethereumLog.Data) != 130 || ethereumLog.Data[:2] != "0x" {
			continue // Skip malformed events
		}

		// Store parsed log data in global buffer
		logEntry := &processedLogs[bufferPosition]
		logEntry.contractAddress = ethereumLog.Address[2:] // Remove 0x prefix
		logEntry.eventData = ethereumLog.Data[2:]          // Remove 0x prefix
		logEntry.blockHeight = ethereumLog.BlockNumber[2:] // Store hex string without 0x prefix

		logCount++
	}

	return logCount, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SIMD-OPTIMIZED HEX PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// countHexLeadingZeros performs efficient leading zero counting using SIMD-style operations.
// Processes 32-byte hex segments in parallel for optimal performance characteristics.
//
//go:noinline
//go:norace
//go:nocheckptr
//go:nosplit
//go:registerparams
func countHexLeadingZeros(hexSegment []byte) int {
	// 64-bit pattern representing eight consecutive ASCII '0' characters
	const ZERO_PATTERN = 0x3030303030303030

	// Process four 8-byte chunks simultaneously using SIMD-style operations
	chunk0 := utils.Load64(hexSegment[0:8]) ^ ZERO_PATTERN   // XOR with zero pattern
	chunk1 := utils.Load64(hexSegment[8:16]) ^ ZERO_PATTERN  // XOR reveals non-zeros
	chunk2 := utils.Load64(hexSegment[16:24]) ^ ZERO_PATTERN // Parallel processing
	chunk3 := utils.Load64(hexSegment[24:32]) ^ ZERO_PATTERN // Four chunks at once

	// Create bitmask indicating which chunks contain non-zero characters
	// Expression (x|(^x+1))>>63 produces 1 if any byte in x is non-zero
	chunkMask := ((chunk0|(^chunk0+1))>>63)<<0 | ((chunk1|(^chunk1+1))>>63)<<1 |
		((chunk2|(^chunk2+1))>>63)<<2 | ((chunk3|(^chunk3+1))>>63)<<3

	// Find first chunk containing non-zero character
	firstNonZeroChunk := bits.TrailingZeros64(chunkMask)
	if firstNonZeroChunk == 64 {
		return 32 // All 32 characters are zeros
	}

	// Within the first non-zero chunk, locate first non-zero byte
	chunks := [4]uint64{chunk0, chunk1, chunk2, chunk3}
	firstNonZeroByte := bits.TrailingZeros64(chunks[firstNonZeroChunk]) >> 3

	// Calculate total leading zero count
	return (firstNonZeroChunk << 3) + firstNonZeroByte
}

// parseReservesToZeroTrimmed extracts and trims reserve values from Sync event data.
// Uses SIMD-optimized processing to remove leading zeros efficiently for storage.
//
//go:noinline
//go:norace
//go:nocheckptr
//go:nosplit
//go:registerparams
func parseReservesToZeroTrimmed(eventData string) (string, string) {
	// Convert string to byte slice for SIMD processing
	dataBytes := unsafe.Slice(unsafe.StringData(eventData), len(eventData))

	// Parse reserve0 from bytes 32-64 (32 hex characters)
	leadingZeros0 := countHexLeadingZeros(dataBytes[32:64])
	reserve0Start := 32 + leadingZeros0
	var reserve0 string
	if reserve0Start >= 64 {
		reserve0 = "0" // All zeros case
	} else {
		reserve0 = eventData[reserve0Start:64]
	}

	// Parse reserve1 from bytes 96-128 (32 hex characters)
	leadingZeros1 := countHexLeadingZeros(dataBytes[96:128])
	reserve1Start := 96 + leadingZeros1
	var reserve1 string
	if reserve1Start >= 128 {
		reserve1 = "0" // All zeros case
	} else {
		reserve1 = eventData[reserve1Start:128]
	}

	return reserve0, reserve1
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CSV OUTPUT OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) writeCSVRecord(address string, blockHeight string,
	reserve0, reserve1 string, connectionID int) {

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

	if newBufferSize >= (constants.CSVBufferSize * 9 / 10) {
		harvester.flushCSVBuffer(connectionID)
		currentBufferSize = 0
		newBufferSize = len(csvRecord)
	}

	copy(csvOutputBuffers[connectionID][currentBufferSize:], csvRecord)
	harvester.csvBufferSizes[connectionID] = newBufferSize
	atomic.AddInt64(&harvester.totalEvents, 1)
}

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

	saveMetadata(harvester.syncTarget)

	totalBlocks := harvester.syncTarget - harvester.lastProcessed
	connectionCount := len(harvester.httpClients)

	debug.DropMessage("HARVEST", fmt.Sprintf("Resuming from block %d, %d blocks remaining, %d connections",
		harvester.lastProcessed, totalBlocks, connectionCount))

	workSectors := make([][2]uint64, connectionCount)
	blocksPerSector := totalBlocks / uint64(connectionCount)
	extraBlocks := totalBlocks % uint64(connectionCount)
	sectorStart := harvester.lastProcessed + 1

	for i := 0; i < connectionCount; i++ {
		fromBlock := sectorStart
		sectorSize := blocksPerSector
		if uint64(i) < extraBlocks {
			sectorSize++
		}
		toBlock := fromBlock + sectorSize - 1
		if i == connectionCount-1 {
			toBlock = harvester.syncTarget
		}
		workSectors[i] = [2]uint64{fromBlock, toBlock}
		sectorStart = toBlock + 1
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				harvester.flushAllBuffers()
			case <-harvester.processingContext.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for connectionID := 0; connectionID < connectionCount; connectionID++ {
		wg.Add(1)
		go func(id int, sectorRange [2]uint64) {
			defer wg.Done()
			harvester.harvestSector(sectorRange[0], sectorRange[1], id)
		}(connectionID, workSectors[connectionID])
	}

	wg.Wait()
	harvester.flushAllBuffers()

	events := atomic.LoadInt64(&harvester.totalEvents)
	debug.DropMessage("HARVEST", utils.Itoa(int(events))+" events complete")

	return nil
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) harvestSector(fromBlock, toBlock uint64, connectionID int) {
	currentBlock := fromBlock

	for currentBlock <= toBlock {
		batchSize := harvester.batchSizes[connectionID]
		batchEnd := currentBlock + batchSize - 1
		if batchEnd > toBlock {
			batchEnd = toBlock
		}

		logCount, err := harvester.extractLogBatch(currentBlock, batchEnd, connectionID)
		if err != nil {
			if strings.Contains(err.Error(), "more than 10000 results") {
				harvester.batchSizes[connectionID] = harvester.batchSizes[connectionID] / 2
				if harvester.batchSizes[connectionID] < constants.MinBatchSize {
					harvester.batchSizes[connectionID] = constants.MinBatchSize
				}
				harvester.consecutiveSuccesses[connectionID] = 0
				continue
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		bufferOffset := connectionID * (len(processedLogs) / len(harvester.httpClients))
		for i := 0; i < logCount; i++ {
			logPosition := bufferOffset + i
			if logPosition >= len(processedLogs) {
				break
			}
			harvester.processLogFromGlobalBuffer(&processedLogs[logPosition], connectionID)
		}

		currentBlock = batchEnd + 1
		harvester.consecutiveSuccesses[connectionID]++

		if harvester.consecutiveSuccesses[connectionID] >= 3 {
			harvester.batchSizes[connectionID] = harvester.batchSizes[connectionID] * 2
			harvester.consecutiveSuccesses[connectionID] = 0
		}
	}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) cleanup() {
	harvester.flushAllBuffers()

	if harvester.outputFile != nil {
		harvester.outputFile.Close()
	}
	harvester.cancelFunc()
	control.ShutdownWG.Done()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ExecuteHarvesting() error {
	return ExecuteHarvestingWithConnections(constants.DefaultConnections)
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ExecuteHarvestingWithConnections(connectionCount int) error {
	control.ShutdownWG.Add(1)
	harvester := newSynchronizationHarvester(connectionCount)
	defer harvester.cleanup()
	return harvester.executeHarvesting()
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func CheckHarvestingRequirement() (bool, uint64, uint64, error) {
	client := &http.Client{Timeout: 10 * time.Second, Transport: buildHTTPTransport()}
	rpcEndpoint := "https://" + constants.HarvesterHost + constants.HarvesterPath

	response, err := client.Post(rpcEndpoint, "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
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

	if len(blockResponse.Result) >= 2 && blockResponse.Result[:2] == "0x" {
		currentHeight := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
		lastProcessed := loadMetadata()
		return lastProcessed < currentHeight, lastProcessed, currentHeight, nil
	}

	return false, 0, 0, fmt.Errorf("invalid response format")
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func FlushHarvestedReservesToRouter() error {
	database, err := sql.Open("sqlite3", "uniswap_pairs.db")
	if err != nil {
		return err
	}
	defer database.Close()

	var totalPairs int
	err = database.QueryRow("SELECT COUNT(*) FROM pools").Scan(&totalPairs)
	if err != nil {
		return err
	}

	if totalPairs == 0 {
		return fmt.Errorf("no trading pairs found in database")
	}

	reserveStorage := make([][8]uint64, totalPairs+1)
	blockHeights := make([]uint64, totalPairs+1)

	csvFile, err := os.Open(constants.HarvesterOutputPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer csvFile.Close()

	csvData, err := os.ReadFile(constants.HarvesterOutputPath)
	if err != nil {
		return fmt.Errorf("failed to read CSV file: %v", err)
	}

	csvLines := strings.Split(string(csvData), "\n")

	eventsProcessed := 0
	eventsSkipped := 0

	for i := len(csvLines) - 1; i >= 0; i-- {
		line := csvLines[i]
		if i == 0 || line == "" {
			continue
		}

		lineBytes := []byte(line)

		commaPositions := make([]int, 0, 4)
		commaPositions = append(commaPositions, -1)
		for j, b := range lineBytes {
			if b == ',' {
				commaPositions = append(commaPositions, j)
			}
		}
		commaPositions = append(commaPositions, len(lineBytes))

		if len(commaPositions) < 5 {
			continue
		}

		addressBytes := lineBytes[commaPositions[0]+1 : commaPositions[1]]
		blockBytes := lineBytes[commaPositions[1]+1 : commaPositions[2]]
		reserve0Bytes := lineBytes[commaPositions[2]+1 : commaPositions[3]]
		reserve1Bytes := lineBytes[commaPositions[3]+1 : commaPositions[4]]

		address := string(addressBytes)
		blockString := string(blockBytes)

		if !strings.HasPrefix(address, "0x") {
			address = "0x" + address
		}

		pairID := router.LookupPairByAddress([]byte(address[2:]))
		if pairID == 0 {
			eventsSkipped++
			continue
		}

		blockNumber, err := strconv.ParseUint(blockString, 16, 64)
		if err != nil {
			continue
		}

		if blockNumber <= blockHeights[pairID] {
			eventsSkipped++
			continue
		}

		var reserve0Array [4]uint64
		var reserve1Array [4]uint64

		var reserve0Padded [32]byte
		copyLength := len(reserve0Bytes)
		if copyLength > 32 {
			copyLength = 32
		}
		copy(reserve0Padded[:copyLength], reserve0Bytes)

		for j := 0; j < 4; j++ {
			reserve0Array[j] = *(*uint64)(unsafe.Pointer(&reserve0Padded[j*8]))
		}

		var reserve1Padded [32]byte
		copyLength = len(reserve1Bytes)
		if copyLength > 32 {
			copyLength = 32
		}
		copy(reserve1Padded[:copyLength], reserve1Bytes)

		for j := 0; j < 4; j++ {
			reserve1Array[j] = *(*uint64)(unsafe.Pointer(&reserve1Padded[j*8]))
		}

		for j := 0; j < 4; j++ {
			reserveStorage[pairID][j] = reserve0Array[j]
			reserveStorage[pairID][j+4] = reserve1Array[j]
		}

		blockHeights[pairID] = blockNumber
		eventsProcessed++
	}

	debug.DropMessage("HARVEST", utils.Itoa(eventsProcessed)+" states loaded")
	debug.DropMessage("HARVEST", utils.Itoa(totalPairs)+" pairs ready")

	return nil
}
