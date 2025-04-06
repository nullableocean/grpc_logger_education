package admin

import (
	"errors"
	"sync"
	"time"
)

type Stat struct {
	ByMethods   map[string]uint64
	ByConsumers map[string]uint64
}

type Statist interface {
	Update(method string, consumer string)
	RegisterInterval(interval uint64) (<-chan *Stat, error)
}

type statist struct {
	intervals      map[uint64]*Stat
	intervalsChans map[uint64]chan *Stat

	mu      *sync.RWMutex
	muStop  *sync.RWMutex
	stop    chan struct{}
	stopped bool
}

func newStatist() *statist {
	return &statist{
		intervals:      map[uint64]*Stat{},
		intervalsChans: map[uint64]chan *Stat{},
		mu:             &sync.RWMutex{},
		stop:           make(chan struct{}),
		muStop:         &sync.RWMutex{},
	}
}

func (s *statist) Update(method string, consumer string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, stat := range s.intervals {
		stat.ByMethods[method] += 1
		stat.ByConsumers[consumer] += 1
	}
}

func (s *statist) RegisterInterval(interval uint64) (<-chan *Stat, error) {
	s.muStop.RLock()
	if s.stopped {
		return nil, errors.New("service statist stopped")
	}
	s.muStop.RUnlock()

	s.mu.Lock()
	ch, ex := s.intervalsChans[interval]
	if ex {
		return ch, nil
	}

	s.intervals[interval] = &Stat{
		ByMethods:   map[string]uint64{},
		ByConsumers: map[string]uint64{},
	}

	ch = make(chan *Stat)
	s.intervalsChans[interval] = ch
	s.mu.Unlock()

	go s.processInterval(interval, ch)

	return ch, nil
}

func (s *statist) processInterval(interval uint64, ch chan<- *Stat) {
	s.muStop.RLock()
	if s.stopped {
		return
	}
	s.muStop.RUnlock()

	ticker := time.NewTicker(time.Duration(interval * uint64(time.Second)))
	for {
		select {
		case <-s.stop:
			close(ch)

			s.mu.Lock()
			delete(s.intervalsChans, interval)
			s.mu.Unlock()

			return
		case <-ticker.C:
			s.mu.RLock()
			stat := s.intervals[interval]
			s.mu.RUnlock()

			go s.cleanIntervalStat(interval)
			ch <- stat
		}
	}
}

func (s *statist) cleanIntervalStat(interval uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ex := s.intervals[interval]; !ex {
		return
	}

	s.intervals[interval] = &Stat{
		ByMethods:   map[string]uint64{},
		ByConsumers: map[string]uint64{},
	}
}

func (s *statist) Stop() {
	s.muStop.Lock()
	close(s.stop)
	s.stopped = true
	s.muStop.Unlock()
}
