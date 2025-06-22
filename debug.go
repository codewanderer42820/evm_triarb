package main

import "log"

// dropError keeps the hot path allocation-free by avoiding fmt.Printf.
// • When err ≠ nil  →  "prefix: err".
// • When err == nil →  just "prefix" (acts like a lightweight trace).
func dropError(prefix string, err error) {
	if err != nil {
		log.Printf("%s: %v", prefix, err)
	} else {
		log.Print(prefix)
	}
}
