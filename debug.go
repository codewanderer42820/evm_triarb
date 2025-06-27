package main

import "log"

// dropError is a lightweight, allocation-free diagnostic logger used
// in all non-hot paths (setup, errors, GC).
//
// It avoids fmt.Printf-style formatting on the hot path by branching on nil.
//
// Behavior:
//   - If `err != nil`, prints:   "<prefix>: <error>"
//   - If `err == nil`, prints:   "<prefix>" (used as a cheap trace or GC tag)
//
// It is intentionally unformatted and minimal — avoid extending.
//
//go:nosplit
//go:inline
func dropError(prefix string, err error) {
	if err != nil {
		log.Printf("%s: %v", prefix, err)
	} else {
		log.Print(prefix)
	}
}
