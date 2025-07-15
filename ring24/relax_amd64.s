// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ CPU RELAXATION - ASSEMBLY IMPLEMENTATION
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Trading System
// Component: x86-64 Assembly Language Support
//
// Description:
//   Low-level assembly implementation of CPU relaxation using the PAUSE instruction.
//   Provides direct hardware access for optimal spin-wait behavior on x86-64 processors.
//
// Technical Details:
//   - PAUSE instruction: Introduced in Pentium 4 for spin-wait optimization
//   - Execution delay: Typically 10-140 cycles depending on processor
//   - Pipeline hint: Prevents memory order speculation during loops
//   - Power state: Allows transition to lower power consumption
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

//go:build amd64 && !noasm

#include "textflag.h"

// cpuRelax emits PAUSE instruction for cooperative busy-wait loops.
// 
// INSTRUCTION ENCODING:
//   PAUSE is encoded as F3 90 (REP NOP) for backward compatibility.
//   Older processors treat it as a no-op, newer ones implement delays.
//
// PERFORMANCE IMPACT:
//   - Reduces power consumption by 10-30% in tight loops
//   - Improves sibling hyperthread throughput by up to 50%
//   - Prevents pipeline flushes from memory ordering speculation
//
TEXT ·cpuRelax(SB), NOSPLIT, $0
	PAUSE
	RET