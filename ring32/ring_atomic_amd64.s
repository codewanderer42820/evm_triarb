//go:build amd64 && !noasm
// ring_atomic_amd64.s
//
// Hand-written wrappers around MOVQ that satisfy acquire/release semantics
// and act as compiler barriers, avoiding the heavier LOCK-prefixed ops.
//
#include "textflag.h"

// func loadAcquireUint64(p *uint64) (v uint64)
TEXT ·loadAcquireUint64(SB), NOSPLIT, $0-16
	MOVQ p+0(FP), AX
	MOVQ (AX), AX
	MOVQ AX, v+8(FP)
	RET

// func storeReleaseUint64(p *uint64, v uint64)
TEXT ·storeReleaseUint64(SB), NOSPLIT, $0-24
	MOVQ p+0(FP), AX
	MOVQ v+8(FP), DX
	MOVQ DX, (AX)
	RET
