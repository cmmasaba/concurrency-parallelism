// The gas-station command simulates concurrency is the real world
// The pipeline consists of four stages: fueling, air pumping, windshield
// cleaning and finally payment. For simplicity, the payment stage will be
// prompted by the gas station operator.
// Each stage will thus have it's respective machine: fuel pump, air pump,
// cleaner, payment machine
//
// This simulation will examine aspects like throughput, latency and utilization

package main

import (
	"flag"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	mode = flag.String("mode", "ideal", `comma-separated list of modes:
	ideal: no synchronization and no contention overhead. Fails the race detector.
	locking: one lock leading to maximal contention.
	finelocking: one lock per machine allowing parallelism.
	parclean: finelocking with windshield cleaning happening in parallel with other stages.
	parpump: finelocking with air pumping happening in parallel with other stages.
	parcleanpump: finelocking with parclean and parpump.
	noclean: skip the windshield cleaning stage.
	nopump: skip the air pumping stage.
	linearpipe-N: a pipeline with one goroutine per machine.
	splitpipe-N: a pipeline with parclean and parpump stages in parallel.
	multi-N: finelocking but with N copies of each machine:
	multipipe-N: N copies of linearpipe.
	`)
	duration       = flag.Duration("duration", 1*time.Second, "performance test duration")
	interval       = flag.Duration("interval", 0, "performance test request interval")
	fuelTime       = flag.Duration("fuel", 2*time.Millisecond, "fuel phase duration")
	cleanTime      = flag.Duration("clean", 1*time.Millisecond, "clean phase duration")
	pumpTime       = flag.Duration("pump", 1*time.Millisecond, "pump phase duration")
	payTime        = flag.Duration("pay", 1*time.Millisecond, "payment phase duration")
	jitter         = flag.Duration("jitter", 0, "add uniform random duration to each phase")
	printDurations = flag.Bool("printdurs", false, "print duration distribution for each phase")
	trace          = flag.String("trace", "", "execution trace file i.e. ../trace.out")
	header         = flag.Bool("header", true, "whether to print CSV header")
	pars           intList
	maxqs          intList
)

func init() {
	flag.Var(&pars, "par", "comma-separated list of perf test parallelism (how many gas stations to run in parallel)")
	flag.Var(&maxqs, "maxq", "comma-separated max lengths of the request queue (how many calls to queue up)")
}

type intList []int

func (il *intList) Set(s string) error {
	ss := strings.Split(s, ",")
	for _, s := range ss {
		n, err := strconv.Atoi(s)
		if err != nil {
			return err
		}

		*il = append(*il, n)
	}

	return nil
}

func (il *intList) String() string {
	var ss []string
	for _, n := range *il {
		ss = append(ss, strconv.Itoa(n))
	}

	return strings.Join(ss, ",")
}

type machine struct {
	name string
	sync.Mutex
	*sampler
}

func newMachine(name string) *machine {
	return &machine{
		name:    name,
		sampler: newSampler(),
	}
}

func (m *machine) close() {
	if m == nil {
		return
	}

	m.sampler.close()
	if *printDurations {
		log.Println(m.name, ":", m.sampler)
	}
}

var (
	fuelPump = newMachine("fuelPump")
	airPump  = newMachine("airPump")
	cleaner  = newMachine("cleaner")
)

func resetMachines() {
	fuelPump.close()
	airPump.close()
	cleaner.close()
	fuelPump = newMachine("fuelPump")
	airPump = newMachine("airPump")
	cleaner = newMachine("cleaner")
}

func runPhase(d, jitter time.Duration) time.Duration {
	if jitter > 0 {
		// add uniform random duration in [-jitter/2, jitter/2]
		d += time.Duration(rand.Int63n((jitter).Nanoseconds()))
		d -= jitter / 2
	}
	start := time.Now()
	useCPU(d)
	return time.Since(start)
}

func pumpFuel(fuelPump *machine) {}

func pumpAir(airPump *machine) {}

func cleanCar (cleaner *machine) {}

func promptPayment() {}
