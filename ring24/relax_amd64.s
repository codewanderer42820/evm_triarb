
// ============================================================================

// relax_amd64.s - x86-64 Assembly Implementation
// ============================================================================
//
// Assembly language implementation of CPU relaxation for x86-64 architecture.
// Provides hardware-level optimization for busy-wait scenarios.
//
// Instruction details:
//   - PAUSE: x86-64 hint instruction for spin-wait optimization
//   - Power reduction: Signals processor to reduce power consumption
//   - SMT efficiency: Allows better resource sharing in hyperthreaded cores
//   - Cache behavior: Reduces unnecessary cache line bouncing
//
// Assembly code:
//go:build amd64 && !noasm

#include "textflag.h"

// cpuRelax emits PAUSE instruction for cooperative busy-wait loops.
// Optimizes power consumption and SMT performance during active polling.
TEXT ·cpuRelax(SB), NOSPLIT, $0
	PAUSE
	RET
