package router

import "main/bucketqueue"

// Fanout lives in CoreRouter.Fanouts[lid][j] .
//
//	Path  ─ points at the shared triangle object (ArbPath).
//	Queue ─ bucket queue owned by *this pair* (tickSoA.Queue).
//	Idx   ─ row in tickSoA.t0/t1/t2  (SoA fast path).
//	Edge  ─ 0,1,2 = which leg inside Path the *pair* represents.
type Fanout struct {
	Path  *ArbPath           //  8 B
	Queue *bucketqueue.Queue //  8 B
	Idx   uint32             //  4 B  (≤ 2^32 fan-outs per pair)
	Edge  uint16             //  2 B
	_pad  uint16             //  2 B  -> struct size = 32 B
}
