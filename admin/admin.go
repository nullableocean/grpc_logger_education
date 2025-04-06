package admin

import "sync"

type Admin struct {
	logger            *loggist
	stat              *statist
	eventChan         chan Event
	observeEventRuned bool
	stop              chan struct{}
	stoped            bool
	muStop            *sync.Mutex
	muObs             *sync.Mutex
}

func NewAdminService() *Admin {
	return &Admin{
		logger:    newLoggist(),
		eventChan: make(chan Event),
		stat:      newStatist(),
		stop:      make(chan struct{}),
		muStop:    &sync.Mutex{},
		muObs:     &sync.Mutex{},
	}
}

func (a *Admin) Logger() Logger {
	return a.logger
}

func (a *Admin) Statist() Statist {
	return a.stat
}

func (a *Admin) Events() <-chan Event {
	a.muObs.Lock()
	defer a.muObs.Unlock()

	if a.observeEventRuned {
		return a.eventChan
	}

	a.observeEventRuned = true
	go a.observeLogEvents()

	return a.eventChan
}

func (a *Admin) observeLogEvents() {
	a.muStop.Lock()
	if a.stoped {
		defer a.muStop.Unlock()
		return
	}
	a.muStop.Unlock()

LOOP:
	for {
		select {
		case <-a.stop:
			break LOOP
		default:
			e, err := a.logger.get()
			if err != nil {
				continue LOOP
			}

			a.muStop.Lock()
			if a.stoped {
				break LOOP
			}
			a.muStop.Unlock()
			a.eventChan <- e
		}
	}

	a.muObs.Lock()
	close(a.eventChan)
	a.observeEventRuned = false
	a.muObs.Unlock()
}

func (a *Admin) Stop() {
	a.muStop.Lock()
	defer a.muStop.Unlock()

	close(a.stop)
	a.logger.Stop()
	a.stat.Stop()
	a.stoped = true
}
