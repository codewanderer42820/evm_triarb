// Package router provides ultra-low-latency per-core fanout of price updates for arbitrage paths.
//
// Key Components:
//
//   - InitCPURings: pins a consumer goroutine per core, each owning a CoreRouter
//   - RegisterPair + RegisterRoute: configure address → pairID and pairID → CPU mask mappings
//   - RegisterCycles: declaratively injects arbitrage cycles into all routers
//   - RouteUpdate: parses log events and dispatches them to all eligible cores
//
// CoreRouter performs:
//
//   - Tick ingestion via PriceUpdate events
//   - Per-pair bucket queue routing
//   - Fan-out mutation of arbitrage paths and re-prioritization
//
// Threading Model:
//
//   - One producer thread (WebSocket reader) calls RouteUpdate()
//   - Each core has a pinned consumer loop calling onPriceUpdate()
//   - No locks or heap allocations occur after setup
package router
