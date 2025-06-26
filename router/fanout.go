// fanout.go — fan-out entry flattened to 64 bytes for cache line efficiency.
// Path contains ticks (SoA), Edge refers to the triangle leg.

package router

import "main/bucketqueue"

type Fanout struct {
	Pairs [3]PairID // 12 B - cycle (triangle)
	//lint:ignore U1000 "used for cache-line alignment"
	_pad0 [28]byte // padding to align Queue field to 8 bytes

	Queue *bucketqueue.Queue // 8 B - back-pointer to bucket queue
	Edge  uint16             // 2 B - leg (0,1,2) in the triangle
	Idx   uint16             // 2 B - index of tick slice
	//lint:ignore U1000 "used for cache-line alignment"
	_pad1 [6]byte // padding to make total size 64 B
}
