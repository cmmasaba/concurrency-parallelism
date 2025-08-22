// A controlled way to consume CPU resources for specified amount of time
// Useful for testing, benchmarking and simulating CPU-intensive workloads

package main

import (
	"hash/adler32"
	"log"
	"time"
)

// opsPerSec is the number of operations executed per second by useCPU
// If zero, it is initialized during init
// You can manually set it with your desired value
var opsPerSec = 0

// oneOp is a single unit of operation executed on the CPU intended to occupy the CPU utilization fully during its execution
// It uses 500 adler32 checksum calculations and is entirely CPU-bound, no reliance on I/O or memory operations
func oneOp() {
	var b [64]byte

	for range 500 {
		b[0] += byte(adler32.Checksum(b[:]))
	}
}

// Initlization done to determine the number of ops per second that can be done on current hardware
// Does this by running oneOp repeatedly for 1 second to determine the baseline performance metric
func init() {
	if opsPerSec != 0 {
		return // opsPerSec already set
	}

	var (
		d   time.Duration
		ops int
	)
	for {
		start := time.Now()

		oneOp()

		d += time.Since(start)

		ops++
		if d > 1*time.Second {
			opsPerSec = ops
			log.Print("opsPerSec = ", opsPerSec)

			return
		}
	}
}

// useCPU generates load and consumes the CPU for the specified time
// Requires a time duration
func useCPU(d time.Duration) {
	c := opsPerSec * int(d) / int(time.Second)

	for range c {
		oneOp()
	}
}
