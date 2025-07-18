// ════════════════════════════════════════════════════════════════════════════════════════════════
// BARE MINIMUM CSV DUMP SYNCHARVESTER
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Parallel connections, CSV output, minimal code
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"fmt"
	"math/bits"
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
// CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	DeploymentBlock  = uint64(10000835)
	CSVPath          = "uniswap_v2.csv"
	OptimalBatchSize = uint64(5_000)
	MinBatchSize     = uint64(100)
	MaxBatchSize     = uint64(10_000)
	MaxLogSliceSize  = 300_000
	SyncEventSig     = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
	NumConnections   = 6
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
// GLOBAL BUFFERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

var (
	globalRespBuffers [NumConnections][8 * 1024 * 1024]byte
	globalLogBuffer   [MaxLogSliceSize]ChadLog
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
	ctx                  context.Context
	cancel               context.CancelFunc
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Removed - using direct sonnet.Unmarshal

func newChadSync() *ChadSync {
	ctx, cancel := context.WithCancel(context.Background())

	s := &ChadSync{
		url:                  "https://" + constants.WsHost + "/v3/a2a3139d2ab24d59bed2dc3643664126",
		batchSize:            OptimalBatchSize,
		consecutiveSuccesses: 0,
		ctx:                  ctx,
		cancel:               cancel,
	}

	for i := 0; i < NumConnections; i++ {
		s.clients[i] = &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        4,
				MaxIdleConnsPerHost: 4,
				DisableCompression:  true,
			},
		}
	}

	var err error
	s.outputFile, err = os.Create(CSVPath)
	if err != nil {
		panic(err)
	}

	// Write CSV header
	s.outputFile.WriteString("address,block,reserve0,reserve1\n")
	s.outputFile.Sync()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	return s
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RPC OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (s *ChadSync) blockNumber() uint64 {
	reqJSON := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`

	for {
		resp, err := s.clients[0].Post(s.url, "application/json", strings.NewReader(reqJSON))
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		n, _ := resp.Body.Read(globalRespBuffers[0][:256])
		resp.Body.Close()

		if n == 0 {
			time.Sleep(time.Second)
			continue
		}

		var response EthBlockResponse
		err = sonnet.Unmarshal(globalRespBuffers[0][:n], &response)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		if len(response.Result) >= 2 && response.Result[:2] == "0x" {
			blockNum := utils.ParseHexU64([]byte(response.Result[2:]))
			if blockNum > 0 {
				return blockNum
			}
		}

		time.Sleep(time.Second)
	}
}

func (s *ChadSync) getLogs(from, to uint64, connID int) (int, error) {
	reqJSON := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x%x","toBlock":"0x%x","topics":["%s"]}],"id":1}`, from, to, SyncEventSig)

	resp, err := s.clients[connID].Post(s.url, "application/json", strings.NewReader(reqJSON))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	totalBytes := 0
	respBuffer := &globalRespBuffers[connID]

	for {
		n, err := resp.Body.Read(respBuffer[totalBytes:])
		totalBytes += n
		if err != nil || totalBytes >= len(respBuffer)-1 {
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

		log.address = ethLog.Address // Keep full address with 0x
		log.data = ethLog.Data[2:]   // Remove 0x
		log.blockNum = utils.ParseHexU64([]byte(ethLog.BlockNumber[2:]))

		logCount++
	}

	return logCount, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HEX PROCESSING
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
// CSV WRITING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (s *ChadSync) writeCSV(address string, blockNum uint64, reserve0, reserve1 string) {
	s.fileMutex.Lock()
	defer s.fileMutex.Unlock()

	fmt.Fprintf(s.outputFile, "%s,%d,%s,%s\n", address, blockNum, reserve0, reserve1)
	s.outputFile.Sync()
}

func (s *ChadSync) processLogFromGlobal(log *ChadLog) {
	reserve0, reserve1 := parseReservesToZeroTrimmed(log.data)
	s.writeCSV(log.address, log.blockNum, reserve0, reserve1)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN SYNC ENGINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (s *ChadSync) sync() error {
	s.syncTarget = s.blockNumber()
	s.lastProcessed = DeploymentBlock

	if s.lastProcessed >= s.syncTarget {
		return nil
	}

	totalBlocks := s.syncTarget - s.lastProcessed
	blocksPerSector := totalBlocks / uint64(NumConnections)
	extraBlocks := totalBlocks % uint64(NumConnections)

	fmt.Printf("Syncing %d blocks across %d connections\n", totalBlocks, NumConnections)

	sectors := make([][2]uint64, NumConnections)
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
	}

	var wg sync.WaitGroup
	for connID := 0; connID < NumConnections; connID++ {
		wg.Add(1)
		go func(id int, sectorRange [2]uint64) {
			defer wg.Done()
			s.syncSector(sectorRange[0], sectorRange[1], id)
		}(connID, sectors[connID])
	}

	wg.Wait()
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
				batchSize = batchSize / 2
				if batchSize < MinBatchSize {
					batchSize = MinBatchSize
				}
				consecutiveSuccesses = 0
				continue
			}
			return
		}

		baseOffset := connID * (MaxLogSliceSize / NumConnections)
		for i := 0; i < logCount; i++ {
			logPos := baseOffset + i
			if logPos >= len(globalLogBuffer) {
				break
			}
			s.processLogFromGlobal(&globalLogBuffer[logPos])
		}

		current = batchEnd + 1
		consecutiveSuccesses++

		if consecutiveSuccesses >= 3 {
			batchSize = batchSize * 2
			if batchSize > MaxBatchSize {
				batchSize = MaxBatchSize
			}
			consecutiveSuccesses = 0
		}
	}
}

func (s *ChadSync) close() {
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
	client := &http.Client{Timeout: 30 * time.Second}
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
