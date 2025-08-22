package main

import "time"

const maxSamples = 10000

type sampler struct {
	min, max time.Duration
	samples []time.Duration
	count int
	closed bool
}

func newSampler() *sampler {
	return &sampler{
		samples: make([]time.Duration, 0, maxSamples),
	}
}

func (s *sampler) close ()
