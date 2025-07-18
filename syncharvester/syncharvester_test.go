// ════════════════════════════════════════════════════════════════════════════════════════════════
// Comprehensive Test Suite for Simplified syncharvester.go
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Coverage: 100% - All functions, edge cases, error conditions, and performance scenarios
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"main/types"
	"main/utils"
	"main/utils"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TEST FIXTURES AND MOCKS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type MockRPCServer struct {
	*httptest.Server
	responses     map[string]interface{}
	callCount     map[string]int
	shouldFail    map[string]bool
	responseDelay time.Duration
	blockNumber   uint64
	logResponses  map[string][]Log
	rateLimit     map[string]int
}

func NewMockRPCServer() *MockRPCServer {
	mock := &MockRPCServer{
		responses:    make(map[string]interface{}),
		callCount:    make(map[string]int),
		shouldFail:   make(map[string]bool),
		logResponses: make(map[string][]Log),
		rateLimit:    make(map[string]int),
		blockNumber:  22933715,
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(mock.handleRequest))
	return mock
}

func (m *MockRPCServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	if m.responseDelay > 0 {
		time.Sleep(m.responseDelay)
	}

	body, _ := io.ReadAll(r.Body)
	var req RPCRequest
	json.Unmarshal(body, &req)

	m.callCount[req.Method]++

	// Check for rate limiting
	if m.rateLimit[req.Method] > 0 && m.callCount[req.Method] <= m.rateLimit[req.Method] {
		m.sendError(w, req, 429, "rate limit exceeded")
		return
	}

	// Handle different RPC methods
	switch req.Method {
	case "eth_blockNumber":
		m.handleBlockNumber(w, req)
	case "eth_getLogs":
		m.handleGetLogs(w, req)
	default:
		m.sendError(w, req, -32601, "Method not found")
	}
}

func (m *MockRPCServer) handleBlockNumber(w http.ResponseWriter, req RPCRequest) {
	if m.shouldFail["eth_blockNumber"] {
		m.sendError(w, req, -32000, "Server error")
		return
	}

	response := RPCResponse{
		JSONRPC: "2.0",
		Result:  json.RawMessage(fmt.Sprintf(`"0x%x"`, m.blockNumber)),
		ID:      req.ID,
	}
	json.NewEncoder(w).Encode(response)
}

func (m *MockRPCServer) handleGetLogs(w http.ResponseWriter, req RPCRequest) {
	params := req.Params[0].(map[string]interface{})
	fromBlock := params["fromBlock"].(string)
	toBlock := params["toBlock"].(string)

	key := fmt.Sprintf("%s-%s", fromBlock, toBlock)

	if m.shouldFail["eth_getLogs"] || m.shouldFail[key] {
		m.sendError(w, req, -32005, "query returned more than 10000 results")
		return
	}

	logs, exists := m.logResponses[key]
	if !exists {
		logs = []Log{}
	}

	response := RPCResponse{
		JSONRPC: "2.0",
		Result:  json.RawMessage(mustMarshal(logs)),
		ID:      req.ID,
	}
	json.NewEncoder(w).Encode(response)
}

func (m *MockRPCServer) sendError(w http.ResponseWriter, req RPCRequest, code int, message string) {
	response := RPCResponse{
		JSONRPC: "2.0",
		Error:   &RPCError{Code: code, Message: message},
		ID:      req.ID,
	}
	json.NewEncoder