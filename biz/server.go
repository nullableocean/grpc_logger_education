package biz

import (
	"async_logger/pb"
	"context"
)

type BizServer struct {
	pb.UnimplementedBizServer
	service *BizService
}

func NewBizServer(service *BizService) pb.BizServer {
	return &BizServer{
		service: service,
	}
}

func (b *BizServer) Add(ctx context.Context, nothing *pb.Nothing) (*pb.Nothing, error) {
	err := b.service.Add()
	return nothing, err
}

func (b *BizServer) Check(ctx context.Context, nothing *pb.Nothing) (*pb.Nothing, error) {
	err := b.service.Check()
	return nothing, err
}

func (b *BizServer) Test(ctx context.Context, nothing *pb.Nothing) (*pb.Nothing, error) {
	err := b.service.Test()
	return nothing, err
}
