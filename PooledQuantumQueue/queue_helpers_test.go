// Package pooledquantumqueue provides shared test utilities for PooledQuantumQueue testing.
//
// This file contains common test helpers and utilities used across multiple test files
// to avoid code duplication and ensure consistent test setup patterns.

package pooledquantumqueue

// InitializePool properly initializes a pool to unlinked state.
//
// CRITICAL: All pools must be initialized before use with PooledQuantumQueue.
// This function ensures that all Entry instances in the pool are set to a clean,
// unlinked state that PooledQuantumQueue expects.
//
// INITIALIZATION PERFORMED:
//   - tick = -1 (marks entry as unlinked, not in any queue)
//   - prev = nilIdx (clears previous pointer in doubly-linked chain)
//   - next = nilIdx (clears next pointer in doubly-linked chain)
//   - data = 0 (clears any payload data)
//
// USAGE:
//
//	pool := make([]Entry, poolSize)
//	InitializePool(pool)
//	q := New(unsafe.Pointer(&pool[0]))
//
// This function should be called on every pool slice before creating any
// PooledQuantumQueue instances that will use the pool.
func InitializePool(pool []Entry) {
	for i := range pool {
		pool[i].tick = -1     // Mark as unlinked
		pool[i].prev = nilIdx // Clear prev pointer
		pool[i].next = nilIdx // Clear next pointer
		pool[i].data = 0      // Clear data
	}
}
