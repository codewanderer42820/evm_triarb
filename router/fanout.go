package router

import "main/bucketqueue"

// Fanout is 24 B: two pointers + uint16 Edge (+2 B pad).
type Fanout struct {
	Path  *ArbPath           // shared triangle object
	Queue *bucketqueue.Queue // per-pair bucket ring
	Edge  uint16             // 0,1,2 — which leg inside Path
	_pad  uint16             // alignment filler
}
