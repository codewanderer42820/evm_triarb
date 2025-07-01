// relax_amd64.s — Low-level implementation of cpuRelax for x86-64
//
// Emits the PAUSE instruction for cooperative busy-wait loops.
//
//   - Reduces contention between hyperthreads
//   - Lowers power usage while waiting
//   - Helps performance in spin-heavy ISR systems
//
// Called from Go via declaration in relax_amd64.go.
// Must be marked NOSPLIT for stack safety.
//
//go:build amd64 && !noasm

#include "textflag.h"

// cpuRelax emits PAUSE then returns.
// Used as a tight low-latency yield hint.
TEXT ·cpuRelax(SB), NOSPLIT, $0
	PAUSE
	RET
