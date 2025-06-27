//go:build amd64 && !noasm
// relax_amd64.s
//
// Assembly body for cpuRelax on x86-64.  Emits PAUSE (a hardware back-off
// hint) followed by RET.  Marked NOSPLIT to stay callable from nosplit Go.
//
// TEXT ·cpuRelax(SB), NOSPLIT, $0
//     PAUSE
//     RET
#include "textflag.h"

// func cpuRelax()
TEXT ·cpuRelax(SB), NOSPLIT, $0
	PAUSE
	RET
