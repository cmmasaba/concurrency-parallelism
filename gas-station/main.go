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
	"fmt"
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

type (
	fueled  int
	cleaned int
	pumped  int
	served  int
)

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

func pumpFuel(fuelPump *machine) fueled {
	fuelPump.add(runPhase(*fuelTime, *jitter))
	return fueled(0)
}

func pumpAir(airPump *machine) pumped {
	airPump.add(runPhase(*pumpTime, *jitter))
	return pumped(0)
}

func cleanCar(cleaner *machine) cleaned {
	cleaner.add(runPhase(*cleanTime, *jitter))
	return cleaned(0)
}

func promptPayment(cleanedCar cleaned) served {
	runPhase(*payTime, 0)
	return served(int(cleanedCar))
}

// Station here refers to the whole set of machines required to perform these
// tasks
// Ideal case with no contention. Fails the race detector
func idealStation() {
	_ = pumpFuel(fuelPump)
	_ = pumpAir(airPump)
	cleanedCar := cleanCar(cleaner)
	promptPayment(cleanedCar)
}

// Lock the whole station for each operation. Maximum contention
var station sync.Mutex

func lockingStation() {
	station.Lock()
	defer station.Unlock()

	_ = pumpFuel(fuelPump)
	_ = pumpAir(airPump)
	cleanedCar := cleanCar(cleaner)
	promptPayment(cleanedCar)
}

// Fine grain locking, only lock one machine. Reduces contention.
func fineLockingStation() {
	_ = lockingFuel()
	_ = lockingAirPump()
	cleanedCar := lockingClean()
	promptPayment(cleanedCar)
}

func lockingFuel() fueled {
	fuelPump.Lock()
	defer fuelPump.Unlock()

	return pumpFuel(fuelPump)
}

func lockingClean() cleaned {
	cleaner.Lock()
	defer cleaner.Unlock()

	return cleanCar(cleaner)
}

func lockingAirPump() pumped {
	airPump.Lock()
	defer airPump.Unlock()

	return pumpAir(airPump)
}

// Parallelize operations when there's available CPU
// PumpAir, clean car when fueling it.
func parallelAirPumping() served {
	c := make(chan pumped, 1)

	go func() {
		c <- lockingAirPump()
	}()

	_ = lockingFuel()
	cleanedCar := lockingClean()

	<-c

	return promptPayment(cleanedCar)
}

// Skip the air pumpimg stage to avoid CPU burning for that stage
func skipAirPumping() served {
	lockingFuel()

	cleanedCar := lockingClean()

	return promptPayment(cleanedCar)
}

// Simulate scenario for pumping fuel only, skipping air pumping and cleaning
func fuelPumpOnly() served {
	lockingFuel()

	cleanedCar := cleaned(0)

	return promptPayment(cleanedCar)
}

// Multiple machines to reduce contention
var fuelPumps, airPumps, cleaners chan *machine

// newMachines returns a channel containing n machines in its buffer.
func newMachines(name string, n int) chan *machine {
	c := make(chan *machine, n)

	for i := range n {
		c <- newMachine(fmt.Sprintf("%s%d", name, i))
	}

	return c
}

func closeMachines(c chan *machine) {
	close(c)

	for m := range c {
		m.close()
	}
}

func multiFuel() fueled {
	m := <-fuelPumps

	fuel := pumpFuel(m)
	fuelPumps <- m

	return fuel
}

func multiAirPump() pumped {
	m := <-airPumps

	pump := pumpAir(m)
	airPumps <- m

	return pump
}

func multiClean() cleaned {
	m := <-cleaners

	clean := cleanCar(m)
	cleaners <- m

	return clean
}

func multiStation() served {
	multiFuel()
	multiAirPump()

	cleanedCar := multiClean()

	return promptPayment(cleanedCar)
}

// Linear pipeline
type order struct {
	fuel  fueled
	pump  pumped
	clean chan cleaned
}

type linearPipeline struct {
	fuelMachine     *machine
	airPumpMachine  *machine
	cleaningMachine *machine

	orders          chan order
	fueledOrders    chan order
	airPumpedOrders chan order
	done            chan int
}

func newLinearPipeline(buffer int) *linearPipeline {
	p := &linearPipeline{
		fuelMachine:     newMachine("fuelPump"),
		airPumpMachine:  newMachine("airPump"),
		cleaningMachine: newMachine("cleaner"),
		orders:          make(chan order, buffer),
		fueledOrders:    make(chan order, buffer),
		airPumpedOrders: make(chan order, buffer),
		done:            make(chan int),
	}

	go p.pumpFuel()
	go p.pumpAir()
	go p.clean()

	return p
}

func (p *linearPipeline) station() served {
	o := order{clean: make(chan cleaned, 1)}
	p.orders <- o

	clean := <-o.clean

	return promptPayment(clean)
}

func (p *linearPipeline) pumpFuel() {
	for o := range p.orders {
		o.fuel = pumpFuel(p.fuelMachine)
		p.fueledOrders <- o
	}

	close(p.fueledOrders)
}

func (p *linearPipeline) pumpAir() {
	for o := range p.fueledOrders {
		o.pump = pumpAir(p.airPumpMachine)
		p.airPumpedOrders <- o
	}

	close(p.airPumpedOrders)
}

func (p *linearPipeline) clean() {
	for o := range p.airPumpedOrders {
		o.clean <- cleanCar(p.cleaningMachine)
	}

	close(p.done)
}

func (p *linearPipeline) close() {
	close(p.orders)
	<-p.done

	p.fuelMachine.close()
	p.airPumpMachine.close()
	p.cleaningMachine.close()
}

func newSkipCleaningPipeline(buffer int) *linearPipeline {
	p := &linearPipeline{
		fuelMachine:    newMachine("fuelMachine"),
		airPumpMachine: newMachine("airPumpMachine"),
		orders:         make(chan order, buffer),
		fueledOrders:   make(chan order, buffer),
		done:           make(chan int),
	}

	go p.pumpFuel()
	go p.pumpAir()

	return p
}

func (p *linearPipeline) skipCleaningPipelineStation() served {
	o := order{clean: make(chan cleaned, 1)}

	p.orders <- o

	clean := <-o.clean

	return promptPayment(clean)
}

func (p *linearPipeline) skipCleaningPipelinePumpAir() {
	for o := range p.fueledOrders {
		o.pump = pumpAir(p.airPumpMachine)
		p.airPumpedOrders <- o
	}

	close(p.done)
}

type splitOrder struct {
	fueled  fueled
	pumped  chan pumped
	cleaned chan cleaned
}

type splitPipeline struct {
	fuelingMachine  *machine
	airPumpMachine  *machine
	cleaningMachine *machine

	orders        chan splitOrder
	airPumpOrders chan splitOrder
	airPumpDone   chan int

	cleanOrders chan splitOrder
	cleanerDone chan int
}

func newSplitPipeline(buffer int) *splitPipeline {
	p := &splitPipeline{
		fuelingMachine:  newMachine("fuelMachine"),
		airPumpMachine:  newMachine("airPumpMachine"),
		cleaningMachine: newMachine("cleaningMachine"),
		orders:          make(chan splitOrder, buffer),
		airPumpOrders:   make(chan splitOrder, buffer),
		airPumpDone:     make(chan int),
		cleanOrders:     make(chan splitOrder, buffer),
		cleanerDone:     make(chan int),
	}

	return p
}

func (p *splitPipeline) station() served {
	o := splitOrder{
		pumped:  make(chan pumped, 1),
		cleaned: make(chan cleaned, 1),
	}

	p.airPumpOrders <- o

	p.cleanOrders <- o

	<-o.pumped
	cleaned := <-o.cleaned

	return promptPayment(cleaned)
}

func (p *splitPipeline) pumpFuel() {
	for o := range p.orders {
		o.fueled = pumpFuel(p.fuelingMachine)
		p.airPumpOrders <- o
	}

	close(p.airPumpOrders)
}

func (p *splitPipeline) pumpAir() {
	for o := range p.airPumpOrders {
		o.pumped <- pumpAir(p.airPumpMachine)
	}

	close(p.airPumpDone)
}

func (p *splitPipeline) clean() {
	for o := range p.cleanOrders {
		o.cleaned <- cleanCar(p.cleaningMachine)
	}

	close(p.cleanerDone)
}

func (p *splitPipeline) close() {
	close(p.airPumpOrders)
	<-p.airPumpDone
	close(p.cleanOrders)
	<-p.cleanerDone
	p.fuelingMachine.close()
	p.airPumpMachine.close()
	p.cleaningMachine.close()
}
