package main

import (
	"async_logger/admin"
	"async_logger/biz"
	"async_logger/pb"
	"context"
	"encoding/json"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type ACLTable map[string][]string

func (acl ACLTable) HasAccess(consumer, method string) bool {
	allowedMethods, ok := acl[consumer]
	if !ok {
		return false
	}

	for _, allowedMethod := range allowedMethods {
		if allowedMethod == method ||
			(strings.HasSuffix(allowedMethod, "/*") &&
				strings.HasPrefix(method, strings.TrimSuffix(allowedMethod, "/*"))) {
			return true
		}
	}

	return false
}

func StartMicroservice(ctx context.Context, listenAddr, ACLData string) error {
	var acl ACLTable
	err := json.Unmarshal([]byte(ACLData), &acl)
	if err != nil {
		return err
	}

	adminService := admin.NewAdminService()
	adminSer := admin.NewAdminServer(ctx, adminService)
	bizSer := biz.NewBizServer(biz.NewBizService())

	logger := adminService.Logger()
	statist := adminService.Statist()

	s := grpc.NewServer(
		grpc.ChainStreamInterceptor(StreamLogsInterceptor(logger, statist), StreamAclInterceptor(acl)),
		grpc.ChainUnaryInterceptor(UnaryLogsINterceptor(logger, statist), UnaryAclInterceptor(acl)),
	)

	pb.RegisterAdminServer(s, adminSer)
	pb.RegisterBizServer(s, bizSer)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	go func() {
		s.Serve(lis)
	}()

	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	return nil
}

func StreamAclInterceptor(acl ACLTable) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "metadata is not provided")
		}

		consumers := md.Get(pb.ConsumerMetaKey)
		if len(consumers) == 0 {
			return status.Error(codes.Unauthenticated, "consumer is not provided")
		}
		consumer := consumers[0]

		if !acl.HasAccess(consumer, info.FullMethod) {
			return status.Error(codes.Unauthenticated, "consumer unathenticated")
		}

		return handler(srv, ss)
	}
}

func UnaryAclInterceptor(acl ACLTable) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "metadata is not provided")
		}

		consumers := md.Get("consumer")
		if len(consumers) == 0 {
			return nil, status.Error(codes.Unauthenticated, "consumer is not provided")
		}
		consumer := consumers[0]

		if !acl.HasAccess(consumer, info.FullMethod) {
			return nil, status.Error(codes.Unauthenticated, "consumer unathenticated")
		}

		return handler(ctx, req)
	}
}

func StreamLogsInterceptor(logger admin.Logger, statist admin.Statist) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "metadata is not provided")
		}

		consumers := md.Get(pb.ConsumerMetaKey)
		if len(consumers) == 0 {
			return status.Error(codes.Unauthenticated, "consumer is not provided")
		}
		consumer := consumers[0]

		p, ok := peer.FromContext(ss.Context())
		logger.Log(admin.Event{
			Timestamp: time.Now().Unix(),
			Consumer:  consumer,
			Method:    info.FullMethod,
			Host:      p.Addr.String(),
		})

		statist.Update(info.FullMethod, consumer)

		return handler(srv, ss)
	}
}

func UnaryLogsINterceptor(logger admin.Logger, statist admin.Statist) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "metadata is not provided")
		}

		consumers := md.Get(pb.ConsumerMetaKey)
		if len(consumers) == 0 {
			return nil, status.Error(codes.Unauthenticated, "consumer is not provided")
		}
		consumer := consumers[0]

		p, ok := peer.FromContext(ctx)
		logger.Log(admin.Event{
			Timestamp: time.Now().Unix(),
			Consumer:  consumer,
			Method:    info.FullMethod,
			Host:      p.Addr.String(),
		})

		statist.Update(info.FullMethod, consumer)

		return handler(ctx, req)
	}
}
