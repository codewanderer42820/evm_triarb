// ════════════════════════════════════════════════════════════════════════════════════════════════
// PERFECTLY TUNED CSV DUMP SYNCHARVESTER
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Optimized for perfect balance: not too big, not too small, just right
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"fmt"
	"math/bits"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"main/constants"
	"main/control"
	"main/utils"

	"github.com/sugawarayuuta/sonnet"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PERFECTLY BALANCED CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	DeploymentBlock  = uint64(10000835)
	CSVPath          = "uniswap_v2.csv"
	OptimalBatchSize = uint64(5_000) // Starting point
	MinBatchSize     = uint64(1)     // Only constraint: at least 1
	MaxLogSliceSize  = 200_000       // Balanced memory usage
	SyncEventSig     = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
	NumConnections   = 8 // Perfect balance for most networks

	// Buffer sizes - perfectly tuned
	ResponseBufferSize = 4 * 1024 * 1024 // 4MB - enough for large responses
	CSVBufferSize      = 256 * 1024      // 256KB - good batch writing size
	ReadBufferSize     = 32 * 1024       // 32KB - optimal TCP read size
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// JSON STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type EthLog struct {
	Address     string `json:"address"`
	Data        string `json:"data"`
	BlockNumber string `json:"blockNumber"`
}

type EthLogsResponse struct {
	Result []EthLog `json:"result"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type EthBlockResponse struct {
	Result string `json:"result"`
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PERFECTLY SIZED GLOBAL BUFFERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

var (
	globalRespBuffers [NumConnections][ResponseBufferSize]byte
	globalLogBuffer   [MaxLogSliceSize]ChadLog
	csvBuffers        [NumConnections][CSVBufferSize]byte
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type ChadLog struct {
	address  string
	data     string
	blockNum uint64
}

type ChadSync struct {
	syncTarget           uint64
	lastProcessed        uint64
	batchSize            uint64
	consecutiveSuccesses int
	clients              [NumConnections]*http.Client
	url                  string
	outputFile           *os.File
	fileMutex            sync.Mutex
	csvBufferSizes       [NumConnections]int
	ctx                  context.Context
	cancel               context.CancelFunc
	totalEvents          int64
	currentBlock         uint64 // Track current highest block processed
	startTime            time.Time
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PERFECTLY TUNED HTTP CLIENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func createBalancedTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second, // Not too aggressive
			KeepAlive: 30 * time.Second, // Standard keep-alive
		}).DialContext,
		MaxIdleConns:          100,              // Reasonable connection pool
		MaxIdleConnsPerHost:   25,               // Perfect per-host limit
		MaxConnsPerHost:       50,               // Enough but not excessive
		IdleConnTimeout:       90 * time.Second, // Long enough to reuse
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second, // Allow for network latency
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,  // Raw speed
		DisableKeepAlives:     false, // Reuse connections
		ForceAttemptHTTP2:     true,
	}
}

func newChadSync() *ChadSync {
	fmt.Println("⚖️ Initializing PERFECTLY BALANCED sync engine...")

	ctx, cancel := context.WithCancel(context.Background())

	s := &ChadSync{
		url:                  "https://" + constants.WsHost + "/v3/a2a3139d2ab24d59bed2dc3643664126",
		batchSize:            OptimalBatchSize,
		consecutiveSuccesses: 0,
		ctx:                  ctx,
		cancel:               cancel,
		startTime:            time.Now(),
	}

	// Create perfectly balanced HTTP clients
	transport := createBalancedTransport()
	for i := 0; i < NumConnections; i++ {
		s.clients[i] = &http.Client{
			Timeout:   45 * time.Second, // Generous but not infinite
			Transport: transport,
		}
	}

	var err error
	s.outputFile, err = os.Create(CSVPath)
	if err != nil {
		panic(err)
	}

	// Write CSV header
	s.outputFile.WriteString("address,block,reserve0,reserve1\n")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Start balanced stats reporter
	go s.reportStats()

	return s
}

func (s *ChadSync) reportStats() {
	ticker := time.NewTicker(5 * time.Second) // Every 5 seconds - not too spammy
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(s.startTime)
			eventsPerSec := float64(s.totalEvents) / elapsed.Seconds()
			blocksProcessed := s.currentBlock - s.lastProcessed
			totalBlocks := s.syncTarget - s.lastProcessed
			progress := float64(blocksProcessed) / float64(totalBlocks) * 100

			fmt.Printf("📊 %d events | %.1f/sec | Block %d (%.1f%%) | %v elapsed\n",
				s.totalEvents, eventsPerSec, s.currentBlock, progress, elapsed.Truncate(time.Second))
		case <-s.ctx.Done():
			return
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BALANCED RPC OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (s *ChadSync) blockNumber() uint64 {
	reqJSON := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`

	for {
		resp, err := s.clients[0].Post(s.url, "application/json", strings.NewReader(reqJSON))
		if err != nil {
			time.Sleep(500 * time.Millisecond) // Reasonable retry delay
			continue
		}

		n, _ := resp.Body.Read(globalRespBuffers[0][:512])
		resp.Body.Close()

		if n == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var response EthBlockResponse
		err = sonnet.Unmarshal(globalRespBuffers[0][:n], &response)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if len(response.Result) >= 2 && response.Result[:2] == "0x" {
			blockNum := utils.ParseHexU64([]byte(response.Result[2:]))
			if blockNum > 0 {
				return blockNum
			}
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (s *ChadSync) getLogs(from, to uint64, connID int) (int, error) {
	reqJSON := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x%x","toBlock":"0x%x","topics":["%s"]}],"id":1}`, from, to, SyncEventSig)

	resp, err := s.clients[connID].Post(s.url, "application/json", strings.NewReader(reqJSON))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// Read response in optimal chunks
	totalBytes := 0
	respBuffer := &globalRespBuffers[connID]
	buf := make([]byte, ReadBufferSize) // 32KB optimal read size

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			copy(respBuffer[totalBytes:], buf[:n])
			totalBytes += n
		}
		if err != nil || totalBytes >= len(respBuffer)-ReadBufferSize {
			break
		}
	}

	if totalBytes == 0 {
		return 0, fmt.Errorf("empty response")
	}

	return s.parseLogsWithSonnet(respBuffer[:totalBytes], connID)
}

func (s *ChadSync) parseLogsWithSonnet(jsonBytes []byte, connID int) (int, error) {
	baseOffset := connID * (MaxLogSliceSize / NumConnections)
	logCount := 0
	maxLogs := MaxLogSliceSize / NumConnections

	var response EthLogsResponse
	err := sonnet.Unmarshal(jsonBytes, &response)
	if err != nil {
		return 0, err
	}

	if response.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", response.Error.Message)
	}

	for _, ethLog := range response.Result {
		if logCount >= maxLogs {
			break
		}

		bufferPos := baseOffset + logCount
		if bufferPos >= len(globalLogBuffer) {
			break
		}

		log := &globalLogBuffer[bufferPos]

		// Validate data field is exactly 130 bytes (0x + 128 hex chars)
		if len(ethLog.Data) != 130 || ethLog.Data[:2] != "0x" {
			continue
		}

		log.address = ethLog.Address[2:] // Remove 0x prefix
		log.data = ethLog.Data[2:]       // Remove 0x
		log.blockNum = utils.ParseHexU64([]byte(ethLog.BlockNumber[2:]))

		logCount++
	}

	return logCount, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// OPTIMIZED HEX PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func countHexLeadingZeros(segment []byte) int {
	const ZERO_PATTERN = 0x3030303030303030

	c0 := utils.Load64(segment[0:8]) ^ ZERO_PATTERN
	c1 := utils.Load64(segment[8:16]) ^ ZERO_PATTERN
	c2 := utils.Load64(segment[16:24]) ^ ZERO_PATTERN
	c3 := utils.Load64(segment[24:32]) ^ ZERO_PATTERN

	mask := ((c0|(^c0+1))>>63)<<0 | ((c1|(^c1+1))>>63)<<1 |
		((c2|(^c2+1))>>63)<<2 | ((c3|(^c3+1))>>63)<<3

	firstChunk := bits.TrailingZeros64(mask)
	if firstChunk == 64 {
		return 32
	}

	chunks := [4]uint64{c0, c1, c2, c3}
	firstByte := bits.TrailingZeros64(chunks[firstChunk]) >> 3
	result := (firstChunk << 3) + firstByte
	return result
}

func parseReservesToZeroTrimmed(dataStr string) (string, string) {
	// Parse reserve0 (bytes 32-64)
	leadingZeros0 := countHexLeadingZeros([]byte(dataStr[32:64]))
	reserve0Start := 32 + leadingZeros0
	var reserve0 string
	if reserve0Start >= 64 {
		reserve0 = "0"
	} else {
		reserve0 = dataStr[reserve0Start:64]
	}

	// Parse reserve1 (bytes 96-128)
	leadingZeros1 := countHexLeadingZeros([]byte(dataStr[96:128]))
	reserve1Start := 96 + leadingZeros1
	var reserve1 string
	if reserve1Start >= 128 {
		reserve1 = "0"
	} else {
		reserve1 = dataStr[reserve1Start:128]
	}

	return reserve0, reserve1
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PERFECTLY BALANCED CSV WRITING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (s *ChadSync) writeCSVBuffered(address string, blockNum uint64, reserve0, reserve1 string, connID int) {
	csvLine := fmt.Sprintf("%s,%d,%s,%s\n", address, blockNum, reserve0, reserve1)

	// Add to connection-specific buffer
	bufferSize := s.csvBufferSizes[connID]
	newSize := bufferSize + len(csvLine)

	// Flush when buffer is 80% full (not 100% for safety)
	if newSize >= (CSVBufferSize * 4 / 5) {
		s.flushCSVBuffer(connID)
		bufferSize = 0
		newSize = len(csvLine)
	}

	copy(csvBuffers[connID][bufferSize:], csvLine)
	s.csvBufferSizes[connID] = newSize
	s.totalEvents++
}

func (s *ChadSync) flushCSVBuffer(connID int) {
	if s.csvBufferSizes[connID] == 0 {
		return
	}

	s.fileMutex.Lock()
	s.outputFile.Write(csvBuffers[connID][:s.csvBufferSizes[connID]])
	s.fileMutex.Unlock()

	s.csvBufferSizes[connID] = 0
}

func (s *ChadSync) flushAllBuffers() {
	for i := 0; i < NumConnections; i++ {
		s.flushCSVBuffer(i)
	}
	s.outputFile.Sync()
}

func (s *ChadSync) processLogFromGlobal(log *ChadLog, connID int) {
	reserve0, reserve1 := parseReservesToZeroTrimmed(log.data)
	s.writeCSVBuffered(log.address, log.blockNum, reserve0, reserve1, connID)

	// Update current block for progress tracking
	if log.blockNum > s.currentBlock {
		s.currentBlock = log.blockNum
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PERFECTLY BALANCED SYNC ENGINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (s *ChadSync) sync() error {
	fmt.Println("📡 Getting current block height...")
	s.syncTarget = s.blockNumber()
	s.lastProcessed = DeploymentBlock

	if s.lastProcessed >= s.syncTarget {
		return nil
	}

	totalBlocks := s.syncTarget - s.lastProcessed
	fmt.Printf("⚖️ BALANCED SYNC: %d blocks across %d connections\n", totalBlocks, NumConnections)
	fmt.Printf("📈 Target: steady, sustainable performance\n")

	// Create perfectly balanced sectors
	sectors := make([][2]uint64, NumConnections)
	blocksPerSector := totalBlocks / uint64(NumConnections)
	extraBlocks := totalBlocks % uint64(NumConnections)
	sectorStart := s.lastProcessed + 1

	for i := 0; i < NumConnections; i++ {
		from := sectorStart
		sectorSize := blocksPerSector
		if uint64(i) < extraBlocks {
			sectorSize++
		}
		to := from + sectorSize - 1
		if i == NumConnections-1 {
			to = s.syncTarget
		}
		sectors[i] = [2]uint64{from, to}
		sectorStart = to + 1
		fmt.Printf("⚖️ Sector %d: %d → %d (%d blocks)\n", i, from, to, to-from+1)
	}

	// Start balanced periodic buffer flushing (every 10 seconds - not too frequent)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.flushAllBuffers()
			case <-s.ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for connID := 0; connID < NumConnections; connID++ {
		wg.Add(1)
		go func(id int, sectorRange [2]uint64) {
			defer wg.Done()
			s.syncSector(sectorRange[0], sectorRange[1], id)
		}(connID, sectors[connID])
	}

	wg.Wait()
	s.flushAllBuffers() // Final flush

	elapsed := time.Since(s.startTime)
	eventsPerSec := float64(s.totalEvents) / elapsed.Seconds()
	fmt.Printf("🏁 PERFECTLY BALANCED: %d events in %v (%.1f/sec)\n", s.totalEvents, elapsed.Truncate(time.Second), eventsPerSec)

	return nil
}

func (s *ChadSync) syncSector(from, to uint64, connID int) {
	current := from
	batchSize := s.batchSize
	consecutiveSuccesses := 0

	for current <= to {
		batchEnd := current + batchSize - 1
		if batchEnd > to {
			batchEnd = to
		}

		logCount, err := s.getLogs(current, batchEnd, connID)
		if err != nil {
			if strings.Contains(err.Error(), "more than 10000 results") {
				// Binary search: divide by 2 when hitting limits
				batchSize = batchSize / 2
				if batchSize < MinBatchSize {
					batchSize = MinBatchSize
				}
				consecutiveSuccesses = 0
				continue
			}
			// Brief pause on error, then retry
			time.Sleep(1 * time.Second)
			continue
		}

		baseOffset := connID * (MaxLogSliceSize / NumConnections)
		for i := 0; i < logCount; i++ {
			logPos := baseOffset + i
			if logPos >= len(globalLogBuffer) {
				break
			}
			s.processLogFromGlobal(&globalLogBuffer[logPos], connID)
		}

		current = batchEnd + 1
		consecutiveSuccesses++

		// Natural convergence via binary search - wait for 3 successes, then 2x growth
		if consecutiveSuccesses >= 3 {
			batchSize = batchSize * 2 // 2x growth for binary search
			consecutiveSuccesses = 0
		}
	}
}

func (s *ChadSync) close() {
	s.flushAllBuffers()
	if s.outputFile != nil {
		s.outputFile.Close()
	}
	s.cancel()
	control.ShutdownWG.Done()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func ExecutePeakSync() error {
	control.ShutdownWG.Add(1)
	s := newChadSync()
	defer s.close()
	return s.sync()
}

func CheckIfPeakSyncNeeded() (bool, uint64, uint64, error) {
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: createBalancedTransport(),
	}
	url := "https://" + constants.WsHost + "/v3/a2a3139d2ab24d59bed2dc3643664126"

	reqJSON := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	resp, err := client.Post(url, "application/json", strings.NewReader(reqJSON))
	if err != nil {
		return false, 0, 0, err
	}
	defer resp.Body.Close()

	var buf [128]byte
	n, _ := resp.Body.Read(buf[:])

	var response EthBlockResponse
	err = sonnet.Unmarshal(buf[:n], &response)
	if err != nil {
		return false, 0, 0, err
	}

	if len(response.Result) >= 2 && response.Result[:2] == "0x" {
		currentHead := utils.ParseHexU64([]byte(response.Result[2:]))
		return true, DeploymentBlock, currentHead, nil
	}
	return false, 0, 0, fmt.Errorf("invalid response")
}

func FlushSyncedReservesToRouter() error {
	return nil
}
