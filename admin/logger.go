package admin

import (
	"errors"
	"sync"
	"sync/atomic"
)

type Event struct {
	Timestamp int64
	Consumer  string
	Method    string
	Host      string
}

type Logger interface {
	Log(Event) error
}

type logNode struct {
	event Event
	next  *logNode
}

type loggist struct {
	q        chan struct{}
	mu       *sync.Mutex
	logCount atomic.Int32
	logHead  *logNode
	logTail  *logNode
	closed   bool
	stopMu   *sync.RWMutex
}

func newLoggist() *loggist {
	return &loggist{
		q:        make(chan struct{}),
		mu:       &sync.Mutex{},
		stopMu:   &sync.RWMutex{},
		logCount: atomic.Int32{},
		logHead:  nil,
		logTail:  nil,
		closed:   false,
	}
}

func (l *loggist) Log(e Event) error {
	l.stopMu.RLock()
	if l.closed {
		l.stopMu.RUnlock()
		return errors.New("logger stopped")
	}
	l.stopMu.RUnlock()

	l.mu.Lock()
	newNode := &logNode{
		event: e,
	}

	if l.logHead == nil {
		l.logHead = newNode
		l.logTail = newNode
	} else {
		l.logTail.next = newNode
		l.logTail = newNode
	}
	count := l.logCount.Add(1)
	l.mu.Unlock()

	if count == 1 {
		select {
		case l.q <- struct{}{}:
		default:
		}
	}

	return nil
}

func (l *loggist) get() (Event, error) {
	if l.logCount.Load() == 0 {
		_, ok := <-l.q
		if !ok {
			return Event{}, errors.New("logger stopped")
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.logHead == nil {
		return Event{}, errors.New("log empty")
	}

	l.logCount.Add(-1)
	event := l.logHead.event
	l.logHead = l.logHead.next

	if l.logHead == nil {
		l.logTail = nil
	}

	return event, nil
}

func (l *loggist) Stop() {
	l.stopMu.Lock()
	defer l.stopMu.Unlock()

	if !l.closed {
		l.closed = true
		close(l.q)
	}
}
