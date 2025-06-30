// relax_amd64.s — x86-64 implementation of cpuRelax using PAUSE
//go:build amd64 && !noasm

#include "textflag.h"

// cpuRelax emits a single PAUSE instruction followed by RET.
// This is the canonical form of low-cost userland spin throttling on x86 CPUs.
//
// The PAUSE instruction helps avoid excessive resource contention
// especially on hyperthreaded cores during tight polling loops.
//
// Marked NOSPLIT to allow safe usage in nosplit Go paths.

// func cpuRelax()
TEXT ·cpuRelax(SB), NOSPLIT, $0
	PAUSE
	RET
