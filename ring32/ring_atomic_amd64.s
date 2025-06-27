// ring_atomic_amd64.s — MOVQ-based acquire/release ops for x86-64
//go:build amd64 && !noasm

#include "textflag.h"

// loadAcquireUint64 returns *p with acquire ordering.
//
// The operation uses plain MOVQ to read a uint64 value from the address held
// in p. On x86-64, TSO guarantees that dependent loads/stores after this load
// will not be reordered before it, fulfilling acquire semantics.
//
// No MFENCE/LOCK needed; acts as compiler+hardware barrier.
//
// func loadAcquireUint64(p *uint64) (v uint64)
TEXT ·loadAcquireUint64(SB), NOSPLIT, $0-16
	MOVQ p+0(FP), AX       // Load input pointer from stack frame
	MOVQ (AX), AX          // Load uint64 value from memory
	MOVQ AX, v+8(FP)       // Store result to return slot
	RET

// storeReleaseUint64 stores v into *p with release ordering.
//
// Writes value v to address held in p. Since x86-64 guarantees write ordering
// with TSO, no fences are required. Compiler sees this as an ordered write.
//
// func storeReleaseUint64(p *uint64, v uint64)
TEXT ·storeReleaseUint64(SB), NOSPLIT, $0-24
	MOVQ p+0(FP), AX       // Load destination pointer
	MOVQ v+8(FP), DX       // Load value to store
	MOVQ DX, (AX)          // Write value to *p
	RET
