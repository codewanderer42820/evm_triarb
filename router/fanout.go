// fanout.go — one record tying a pair-edge to its triangle and queue

package router

import "main/bucketqueue"

// Fanout now fits in 32 bytes and supports >64 K refs per pair.
type Fanout struct {
	Pairs TriCycle // 12 B
	//lint:ignore U1000 padding for cache alignment
	_pad0 uint32 // align next field to 8-byte boundary

	Queue *bucketqueue.Queue // 8  B (offset 16)
	Idx   uint32             // 4  B (offset 24)  — widened from uint16
	Edge  uint16             // 2  B (offset 28)
	//lint:ignore U1000 padding for cache alignment
	_pad1 uint16 // 2  B (offset 30)  — maintain 32-byte size
}
