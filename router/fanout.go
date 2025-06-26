// fanout.go — one record tying a pair-edge to its triangle and queue
package router

import "main/bucketqueue"

// Fanout is a 32-byte, cache-line-friendly struct.
// Idx is now uint32 so a pair can host >64 K fan-outs.
type Fanout struct {
	Pairs TriCycle // 12 B  global pair IDs of the triangle
	_pad0 uint32   //  4 B  align next field

	Queue *bucketqueue.Queue //  8 B  pointer to the pair’s bucket queue
	Idx   uint32             //  4 B  row inside tickSoA.{t0,t1,t2}
	Edge  uint16             //  2 B  0,1,2 which leg of the triangle
	_pad1 uint16             //  2 B  keep struct at 32 B
}
