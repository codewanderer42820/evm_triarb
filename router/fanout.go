// fanout.go — hot-path leg reference, laid out to fit exactly one cache line.
//
// Offsets (bytes):
//   0-39 : Pairs (12 B) + internal padding (28 B)   ← compiler keeps this block
//  40-47 : Queue pointer (*bucketqueue.Queue)
//  48-49 : Edge   (uint16)   0,1,2
//  50-51 : Idx    (uint16)   index into bucket.t*
//  52-63 : padding (align to 64 B)

package router

import (
	"main/bucketqueue"
)

type Fanout struct {
	Pairs [3]PairID // cycle this leg belongs to
	_pad0 [28]byte  // keep Queue ptr 8-byte aligned (total 40 B)

	Queue *bucketqueue.Queue // back-pointer into owning TickBucket.Queue
	Edge  uint16             // which tick slice: 0/1/2
	Idx   uint16             // position inside the slice
	_pad1 [6]byte            // pad → 64 B exactly
}
