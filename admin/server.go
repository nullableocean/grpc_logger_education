package admin

import (
	"async_logger/pb"
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type LogSubscriber struct {
	stream       pb.Admin_LoggingServer
	err          chan error
	disconnected bool
}

type StatSubscriber struct {
	stream       pb.Admin_StatisticsServer
	err          chan error
	disconnected bool
}

type AdminServer struct {
	pb.UnimplementedAdminServer
	admin        *Admin
	loggers      map[string]*LogSubscriber
	staters      map[uint64]map[string]*StatSubscriber
	intervalsRun map[uint64]bool

	muLogs            *sync.RWMutex
	muStats           *sync.RWMutex
	sendingLogRunning bool
	stopLogs          chan struct{}
	stopStaters       chan struct{}
	wait              *sync.WaitGroup
}

func NewAdminServer(ctx context.Context, admin *Admin) pb.AdminServer {
	serv := &AdminServer{
		admin:        admin,
		loggers:      make(map[string]*LogSubscriber),
		staters:      make(map[uint64]map[string]*StatSubscriber),
		intervalsRun: make(map[uint64]bool),

		muLogs:      &sync.RWMutex{},
		muStats:     &sync.RWMutex{},
		stopLogs:    make(chan struct{}),
		stopStaters: make(chan struct{}),
		wait:        &sync.WaitGroup{},
	}

	go func() {
		<-ctx.Done()
		serv.Stop()
	}()

	return serv
}

func (s AdminServer) Logging(n *pb.Nothing, stream pb.Admin_LoggingServer) error {
	ctx := stream.Context()

	md, ex := metadata.FromIncomingContext(ctx)
	if !ex || len(md.Get(pb.ConsumerMetaKey)) == 0 {
		return status.Error(codes.Unauthenticated, "metadata is not provided")
	}
	consumer := md.Get(pb.ConsumerMetaKey)[0]

	sub := &LogSubscriber{
		stream: stream,
		err:    make(chan error, 1),
	}

	err := s.saveLogSubscriber(consumer, sub)
	if err != nil {
		return err
	}

	if !s.sendingLogRunning {
		go s.processLogs()
	}

	for {
		select {
		case <-ctx.Done():
			s.disconnectLogSubscriber(consumer)
			return nil
		case err := <-sub.err:
			s.disconnectLogSubscriber(consumer)
			return err
		case <-s.stopLogs:
			s.disconnectLogSubscriber(consumer)
			return nil
		}
	}
}

func (s AdminServer) Statistics(interval *pb.StatInterval, stream pb.Admin_StatisticsServer) error {
	ctx := stream.Context()
	md, ex := metadata.FromIncomingContext(ctx)
	if !ex || len(md.Get(pb.ConsumerMetaKey)) == 0 {
		return status.Error(codes.Unauthenticated, "metadata is not provided")
	}

	consumer := md.Get(pb.ConsumerMetaKey)[0]
	intvl := interval.IntervalSeconds

	sub := &StatSubscriber{
		stream: stream,
		err:    make(chan error, 1),
	}

	err := s.saveStatSubscriber(intvl, consumer, sub)
	if err != nil {
		return err
	}

	err = s.sendingStat(intvl)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	for {
		select {
		case <-ctx.Done():
			s.disconnectStatSubscriber(intvl, consumer)
			return nil
		case err := <-sub.err:
			s.disconnectStatSubscriber(intvl, consumer)
			return err
		case <-s.stopLogs:
			s.disconnectStatSubscriber(intvl, consumer)
			return nil
		}
	}
}

func (s AdminServer) processLogs() {
	if s.sendingLogRunning {
		return
	}

	s.sendingLogRunning = true
	evCh := s.admin.Events()
	for {
		select {
		case <-s.stopLogs:
			return
		case e := <-evCh:

			s.sendLog(&pb.Event{
				Timestamp: time.Now().Unix(),
				Consumer:  e.Consumer,
				Method:    e.Method,
				Host:      e.Host,
			})
		}
	}
}

func (s AdminServer) sendLog(e *pb.Event) {
	s.muLogs.RLock()
	defer s.muLogs.RUnlock()

	for _, sub := range s.loggers {
		if sub.disconnected {
			continue
		}

		err := sub.stream.Send(e)
		if err != nil {
			sub.err <- err
		}
	}
}

func (s AdminServer) sendingStat(interval uint64) error {
	s.muStats.Lock()
	defer s.muStats.Unlock()

	if s.intervalsRun[interval] {
		return nil
	}

	stCh, err := s.admin.Statist().RegisterInterval(interval)
	if err != nil {
		return err
	}

	s.intervalsRun[interval] = true
	go func() {
		for {
			select {
			case <-s.stopStaters:
				return
			case stat, ok := <-stCh:
				if !ok {
					return
				}

				s.wait.Add(1)
				go s.sendStatForInterval(interval, stat)
			}
		}
	}()

	return nil
}

func (s AdminServer) sendStatForInterval(interval uint64, stat *Stat) {
	s.muStats.RLock()
	defer s.muStats.RUnlock()
	defer s.wait.Done()

	pbStat := &pb.Stat{
		Timestamp:  time.Now().Unix(),
		ByMethod:   stat.ByMethods,
		ByConsumer: stat.ByConsumers,
	}

	for _, sub := range s.staters[interval] {
		if sub.disconnected {
			continue
		}

		err := sub.stream.Send(pbStat)
		if err != nil {
			sub.err <- err
		}
	}
}

func (s AdminServer) saveLogSubscriber(consumer string, sub *LogSubscriber) error {
	s.muLogs.Lock()
	defer s.muLogs.Unlock()

	exSub, ex := s.loggers[consumer]
	if ex && !exSub.disconnected {
		s.muLogs.Unlock()
		return errors.New("consumer already have connect")
	}

	s.loggers[consumer] = sub

	return nil
}

func (s AdminServer) disconnectLogSubscriber(consumer string) {
	s.muLogs.Lock()
	defer s.muLogs.Unlock()

	sub, ex := s.loggers[consumer]
	if !ex {
		return
	}

	sub.disconnected = true
}

func (s AdminServer) saveStatSubscriber(interval uint64, consumer string, sub *StatSubscriber) error {
	s.muStats.Lock()
	defer s.muStats.Unlock()

	if s.staters[interval] == nil {
		s.staters[interval] = make(map[string]*StatSubscriber)
	}

	exSub, ex := s.staters[interval][consumer]
	if ex && !exSub.disconnected {
		s.muStats.Unlock()
		return errors.New("consumer already have connect")
	}

	s.staters[interval][consumer] = sub

	return nil
}

func (s AdminServer) disconnectStatSubscriber(interval uint64, consumer string) {
	s.muStats.Lock()
	defer s.muStats.Unlock()

	sub, ex := s.staters[interval][consumer]
	if !ex {
		return
	}
	sub.disconnected = true
}

func (s AdminServer) Stop() {
	close(s.stopLogs)
	close(s.stopStaters)

	s.wait.Wait()

	s.admin.Stop()
}
