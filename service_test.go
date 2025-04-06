package main

import (
	"async_logger/pb"
	"context"
	"fmt"
	"io"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

const (
	// какой адрес-порт слушать серверу
	listenAddr string = "127.0.0.1:8082"

	// кого по каким методам пускать
	ACLData string = `{
	"logger1":          ["/main.Admin/Logging"],
	"logger2":          ["/main.Admin/Logging"],
	"stat1":            ["/main.Admin/Statistics"],
	"stat2":            ["/main.Admin/Statistics"],
	"biz_user":         ["/main.Biz/Check", "/main.Biz/Add"],
	"biz_admin":        ["/main.Biz/*"],
	"after_disconnect": ["/main.Biz/Add"]
}`
)

// чтобы не было сюрпризов когда где-то не успела преключиться горутина и не успело что-то стортовать
func wait(amout int) {
	time.Sleep(time.Duration(amout) * 10 * time.Millisecond)
}

// утилитарная функция для коннекта к серверу
func getGrpcConn(t *testing.T) *grpc.ClientConn {
	grcpConn, err := grpc.Dial(
		listenAddr,
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("cant connect to grpc: %v", err)
	}
	return grcpConn
}

// получаем контекст с нужнымы метаданными для ACL
func getConsumerCtx(consumerName string) context.Context {
	// ctx, _ := context.WithTimeout(context.Background(), time.Second)
	ctx := context.Background()
	md := metadata.Pairs(
		"consumer", consumerName,
	)
	return metadata.NewOutgoingContext(ctx, md)
}

// получаем контекст с нужнымы метаданными для ACL с возможностью отмены
func getConsumerCtxWithCancel(consumerName string) (context.Context, context.CancelFunc) {
	ctx, cancelFn := context.WithCancel(context.Background())
	md := metadata.Pairs(
		"consumer", consumerName,
	)
	return metadata.NewOutgoingContext(ctx, md), cancelFn
}

// старт-стоп сервера
func TestServerStartStop(t *testing.T) {
	ctx, finish := context.WithCancel(context.Background())
	err := StartMicroservice(ctx, listenAddr, ACLData)
	if err != nil {
		t.Fatalf("cant start server initial: %v", err)
	}
	wait(1)
	finish() // при вызове этой функции ваш сервер должен остановиться и освободить порт
	wait(1)

	// теперь проверим что вы освободили порт и мы можем стартовать сервер ещё раз
	ctx, finish = context.WithCancel(context.Background())
	err = StartMicroservice(ctx, listenAddr, ACLData)
	if err != nil {
		t.Fatalf("cant start server again: %v", err)
	}
	wait(1)
	finish()
	wait(1)
}

// этим тестом мы проверяем что останавливаются все горутины которые были и нет утечек
// некоторый запас ( goroutinesPerTwoIterations*5 ) остаётся на случай рантайм горутин
func TestServerLeak(t *testing.T) {
	//return
	goroutinesStart := runtime.NumGoroutine()
	TestServerStartStop(t)
	goroutinesPerTwoIterations := runtime.NumGoroutine() - goroutinesStart

	goroutinesStart = runtime.NumGoroutine()
	goroutinesStat := []int{}
	for i := 0; i <= 25; i++ {
		TestServerStartStop(t)
		goroutinesStat = append(goroutinesStat, runtime.NumGoroutine())
	}
	goroutinesPerFiftyIterations := runtime.NumGoroutine() - goroutinesStart
	if goroutinesPerFiftyIterations > goroutinesPerTwoIterations*5 {
		t.Fatalf("looks like you have goroutines leak: %+v", goroutinesStat)
	}
}

// ACL (права на методы доступа) парсится корректно
func TestACLParseError(t *testing.T) {
	// finish'а тут нет потому что стартовать у вас ничего не должно если не получилось распаковать ACL
	err := StartMicroservice(context.Background(), listenAddr, "{.;")
	if err == nil {
		t.Fatalf("expacted error on bad acl json, have nil")
	}
}

// ACL (права на методы доступа) работает корректно
func TestACL(t *testing.T) {
	wait(1)
	ctx, finish := context.WithCancel(context.Background())
	err := StartMicroservice(ctx, listenAddr, ACLData)
	if err != nil {
		t.Fatalf("cant start server initial: %v", err)
	}
	wait(1)
	defer func() {
		finish()
		wait(1)
	}()

	conn := getGrpcConn(t)
	defer conn.Close()

	biz := pb.NewBizClient(conn)
	adm := pb.NewAdminClient(conn)

	for idx, ctx := range []context.Context{
		context.Background(),       // нет поля для ACL
		getConsumerCtx("unknown"),  // поле есть, неизвестный консюмер
		getConsumerCtx("biz_user"), // поле есть, нет доступа
	} {
		_, err = biz.Test(ctx, &pb.Nothing{})
		if err == nil {
			t.Fatalf("[%d] ACL fail: expected err on disallowed method", idx)
		} else if code := grpc.Code(err); code != codes.Unauthenticated {
			t.Fatalf("[%d] ACL fail: expected Unauthenticated code, got %v", idx, code)
		}
	}

	// есть доступ
	_, err = biz.Check(getConsumerCtx("biz_user"), &pb.Nothing{})
	if err != nil {
		t.Fatalf("ACL fail: unexpected error: %v", err)
	}
	_, err = biz.Check(getConsumerCtx("biz_admin"), &pb.Nothing{})
	if err != nil {
		t.Fatalf("ACL fail: unexpected error: %v", err)
	}
	_, err = biz.Test(getConsumerCtx("biz_admin"), &pb.Nothing{})
	if err != nil {
		t.Fatalf("ACL fail: unexpected error: %v", err)
	}

	// ACL на методах, которые возвращают поток данных
	logger, err := adm.Logging(getConsumerCtx("unknown"), &pb.Nothing{})
	_, err = logger.Recv()
	if err == nil {
		t.Fatalf("ACL fail: expected err on disallowed method")
	} else if code := grpc.Code(err); code != codes.Unauthenticated {
		t.Fatalf("ACL fail: expected Unauthenticated code, got %v", code)
	}
}

func TestLogging(t *testing.T) {
	ctx, finish := context.WithCancel(context.Background())
	err := StartMicroservice(ctx, listenAddr, ACLData)
	if err != nil {
		t.Fatalf("cant start server initial: %v", err)
	}
	wait(1)
	defer func() {
		finish()
		wait(1)
	}()

	conn := getGrpcConn(t)
	defer conn.Close()

	biz := pb.NewBizClient(conn)
	adm := pb.NewAdminClient(conn)

	logStream1, err := adm.Logging(getConsumerCtx("logger1"), &pb.Nothing{})
	time.Sleep(1 * time.Millisecond)

	logStream2, err := adm.Logging(getConsumerCtx("logger2"), &pb.Nothing{})

	logData1 := []*pb.Event{}
	logData2 := []*pb.Event{}

	wait(1)

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			fmt.Println("looks like you dont send anything to log stream in 3 sec")
			t.Errorf("looks like you dont send anything to log stream in 3 sec")
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 4; i++ {
			evt, err := logStream1.Recv()
			// log.Println("logger 1", evt, err)
			if err != nil {
				t.Errorf("unexpected error: %v, awaiting event", err)
				return
			}
			// evt.Host читайте как evt.RemoteAddr
			if !strings.HasPrefix(evt.GetHost(), "127.0.0.1:") || evt.GetHost() == listenAddr {
				t.Errorf("bad host: %v", evt.GetHost())
				return
			}
			// protobuf добавляет к структуре свои поля, которвые не видны при приведении к строке и при reflect.DeepEqual
			// поэтому берем не оригинал сообщения, а только нужные значения
			logData1 = append(logData1, &pb.Event{Consumer: evt.Consumer, Method: evt.Method})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			evt, err := logStream2.Recv()
			// log.Println("logger 2", evt, err)
			if err != nil {
				t.Errorf("unexpected error: %v, awaiting event", err)
				return
			}
			if !strings.HasPrefix(evt.GetHost(), "127.0.0.1:") || evt.GetHost() == listenAddr {
				t.Errorf("bad host: %v", evt.GetHost())
				return
			}

			// protobuf добавляет к структуре свои поля, которвые не видны при приведении к строке и при reflect.DeepEqual
			// поэтому берем не оригинал сообщения, а только нужные значения
			logData2 = append(logData2, &pb.Event{Consumer: evt.Consumer, Method: evt.Method})
		}
	}()

	biz.Check(getConsumerCtx("biz_user"), &pb.Nothing{})
	time.Sleep(3 * time.Millisecond)

	biz.Check(getConsumerCtx("biz_admin"), &pb.Nothing{})
	time.Sleep(3 * time.Millisecond)

	biz.Test(getConsumerCtx("biz_admin"), &pb.Nothing{})
	time.Sleep(4 * time.Millisecond)

	wg.Wait()

	expectedLogData1 := []*pb.Event{
		{Consumer: "logger1", Method: "/main.Admin/Logging"},
		{Consumer: "logger2", Method: "/main.Admin/Logging"},
		{Consumer: "biz_user", Method: "/main.Biz/Check"},
		{Consumer: "biz_admin", Method: "/main.Biz/Check"},
	}
	expectedLogData2 := []*pb.Event{
		{Consumer: "logger2", Method: "/main.Admin/Logging"},
		{Consumer: "biz_user", Method: "/main.Biz/Check"},
		{Consumer: "biz_admin", Method: "/main.Biz/Check"},
	}

	if !reflect.DeepEqual(logData1, expectedLogData1) {
		t.Fatalf("logs1 dont match\nhave %+v\nwant %+v", logData1, expectedLogData1)
	}
	if !reflect.DeepEqual(logData2, expectedLogData2) {
		t.Fatalf("logs2 dont match\nhave %+v\nwant %+v", logData2, expectedLogData2)
	}
}

func TestStat(t *testing.T) {
	ctx, finish := context.WithCancel(context.Background())
	err := StartMicroservice(ctx, listenAddr, ACLData)
	if err != nil {
		t.Fatalf("cant start server initial: %v", err)
	}
	wait(1)
	defer func() {
		finish()
		wait(2)
	}()

	conn := getGrpcConn(t)
	defer conn.Close()

	biz := pb.NewBizClient(conn)
	adm := pb.NewAdminClient(conn)

	statStream1, err := adm.Statistics(getConsumerCtx("stat1"), &pb.StatInterval{IntervalSeconds: 2})
	wait(1)
	statStream2, err := adm.Statistics(getConsumerCtx("stat2"), &pb.StatInterval{IntervalSeconds: 4})

	mu := &sync.Mutex{}
	stat1 := &pb.Stat{}
	stat2 := &pb.Stat{}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for {
			stat, err := statStream1.Recv()
			if err != nil && err != io.EOF {
				// fmt.Printf("unexpected error %v\n", err)
				return
			} else if err == io.EOF {
				break
			}
			// log.Println("stat1", stat, err)
			mu.Lock()

			// protobuf добавляет к структуре свои поля, которвые не видны при приведении к строке и при reflect.DeepEqual
			// поэтому берем не оригинал сообщения, а только нужные значения
			stat1 = &pb.Stat{
				ByMethod:   stat.ByMethod,
				ByConsumer: stat.ByConsumer,
			}
			mu.Unlock()
		}
	}()
	go func() {
		for {
			stat, err := statStream2.Recv()
			if err != nil && err != io.EOF {
				// fmt.Printf("unexpected error %v\n", err)
				return
			} else if err == io.EOF {
				break
			}
			// log.Println("stat2", stat, err)
			mu.Lock()

			// protobuf добавляет к структуре свои поля, которвые не видны при приведении к строке и при reflect.DeepEqual
			// поэтому берем не оригинал сообщения, а только нужные значения
			stat2 = &pb.Stat{
				ByMethod:   stat.ByMethod,
				ByConsumer: stat.ByConsumer,
			}
			mu.Unlock()
		}
	}()

	wait(1)

	biz.Check(getConsumerCtx("biz_user"), &pb.Nothing{})
	biz.Add(getConsumerCtx("biz_user"), &pb.Nothing{})
	biz.Test(getConsumerCtx("biz_admin"), &pb.Nothing{})

	wait(200) // 2 sec

	expectedStat1 := &pb.Stat{
		ByMethod: map[string]uint64{
			"/main.Biz/Check":        1,
			"/main.Biz/Add":          1,
			"/main.Biz/Test":         1,
			"/main.Admin/Statistics": 1,
		},
		ByConsumer: map[string]uint64{
			"biz_user":  2,
			"biz_admin": 1,
			"stat2":     1,
		},
	}

	mu.Lock()
	if !reflect.DeepEqual(stat1, expectedStat1) {
		t.Fatalf("stat1-1 dont match\nhave %+v\nwant %+v", stat1, expectedStat1)
	}
	mu.Unlock()

	biz.Add(getConsumerCtx("biz_admin"), &pb.Nothing{})

	wait(220) // 2+ sec

	expectedStat1 = &pb.Stat{
		Timestamp: 0,
		ByMethod: map[string]uint64{
			"/main.Biz/Add": 1,
		},
		ByConsumer: map[string]uint64{
			"biz_admin": 1,
		},
	}
	expectedStat2 := &pb.Stat{
		Timestamp: 0,
		ByMethod: map[string]uint64{
			"/main.Biz/Check": 1,
			"/main.Biz/Add":   2,
			"/main.Biz/Test":  1,
		},
		ByConsumer: map[string]uint64{
			"biz_user":  2,
			"biz_admin": 2,
		},
	}

	mu.Lock()
	if !reflect.DeepEqual(stat1, expectedStat1) {
		t.Fatalf("stat1-2 dont match\nhave %+v\nwant %+v", stat1, expectedStat1)
	}
	if !reflect.DeepEqual(stat2, expectedStat2) {
		t.Fatalf("stat2 dont match\nhave %+v\nwant %+v", stat2, expectedStat2)
	}
	mu.Unlock()

	finish()
}

// TestWorkAfterDisconnect almost the same as TestLogging but one logger will disconnect in process
// see comments marked CHANGED
func TestWorkAfterDisconnect(t *testing.T) {
	ctx, finish := context.WithCancel(context.Background())
	err := StartMicroservice(ctx, listenAddr, ACLData)
	if err != nil {
		t.Fatalf("cant start server initial: %v", err)
	}
	wait(1)
	defer func() {
		finish()
		wait(1)
	}()

	conn := getGrpcConn(t)
	defer conn.Close()

	biz := pb.NewBizClient(conn)
	adm := pb.NewAdminClient(conn)

	ctx1, cancel1 := getConsumerCtxWithCancel("logger1")
	logStream1, err := adm.Logging(ctx1, &pb.Nothing{})
	time.Sleep(1 * time.Millisecond)

	logStream2, err := adm.Logging(getConsumerCtx("logger2"), &pb.Nothing{})

	logData1 := []*pb.Event{}
	logData2 := []*pb.Event{}

	wait(1)

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			fmt.Println("looks like you dont send anything to log stream in 3 sec")
			t.Errorf("looks like you dont send anything to log stream in 3 sec")
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 4; i++ {
			evt, err := logStream1.Recv()
			if err != nil {
				t.Errorf("unexpected error: %v, awaiting event", err)
				return
			}

			if !strings.HasPrefix(evt.GetHost(), "127.0.0.1:") || evt.GetHost() == listenAddr {
				t.Errorf("bad host: %v", evt.GetHost())
				return
			}

			// protobuf добавляет к структуре свои поля, которвые не видны при приведении к строке и при reflect.DeepEqual
			// поэтому берем не оригинал сообщения, а только нужные значения
			logData1 = append(logData1, &pb.Event{Consumer: evt.Consumer, Method: evt.Method})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 6; i++ {
			evt, err := logStream2.Recv()
			if err != nil {
				t.Errorf("unexpected error: %v, awaiting event", err)
				return
			}

			if !strings.HasPrefix(evt.GetHost(), "127.0.0.1:") || evt.GetHost() == listenAddr {
				t.Errorf("bad host: %v", evt.GetHost())
				return
			}

			// protobuf добавляет к структуре свои поля, которвые не видны при приведении к строке и при reflect.DeepEqual
			// поэтому берем не оригинал сообщения, а только нужные значения
			logData2 = append(logData2, &pb.Event{Consumer: evt.Consumer, Method: evt.Method})
		}
	}()

	biz.Check(getConsumerCtx("biz_user"), &pb.Nothing{})
	time.Sleep(2 * time.Millisecond)

	biz.Check(getConsumerCtx("biz_admin"), &pb.Nothing{})
	time.Sleep(2 * time.Millisecond)

	biz.Test(getConsumerCtx("biz_admin"), &pb.Nothing{})
	time.Sleep(2 * time.Millisecond)

	// CHANGED
	wait(12)
	cancel1()
	wait(12)

	biz.Add(getConsumerCtx("after_disconnect"), &pb.Nothing{})
	time.Sleep(2 * time.Millisecond)
	biz.Add(getConsumerCtx("after_disconnect"), &pb.Nothing{})
	time.Sleep(2 * time.Millisecond)
	// END CHANGED

	wg.Wait()

	expectedLogData1 := []*pb.Event{
		{Consumer: "logger1", Method: "/main.Admin/Logging"},
		{Consumer: "logger2", Method: "/main.Admin/Logging"},
		{Consumer: "biz_user", Method: "/main.Biz/Check"},
		{Consumer: "biz_admin", Method: "/main.Biz/Check"},
	}
	expectedLogData2 := []*pb.Event{
		{Consumer: "logger2", Method: "/main.Admin/Logging"},
		{Consumer: "biz_user", Method: "/main.Biz/Check"},
		{Consumer: "biz_admin", Method: "/main.Biz/Check"},
		{Consumer: "biz_admin", Method: "/main.Biz/Test"},
		{Consumer: "after_disconnect", Method: "/main.Biz/Add"}, // CHANGED
		{Consumer: "after_disconnect", Method: "/main.Biz/Add"}, // CHANGED
	}

	if !reflect.DeepEqual(logData1, expectedLogData1) {
		t.Fatalf("logs1 dont match\nhave %+v\nwant %+v", logData1, expectedLogData1)
	}
	if !reflect.DeepEqual(logData2, expectedLogData2) {
		t.Fatalf("logs2 dont match\nhave %+v\nwant %+v", logData2, expectedLogData2)
	}
}

func __dummyLog() {
	fmt.Println(1)
	log.Println(1)
}
