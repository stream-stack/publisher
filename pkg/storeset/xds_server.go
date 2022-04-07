package storeset

import (
	"context"
	"fmt"
	"github.com/stream-stack/publisher/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

type XdsServer struct {
}

func (x *XdsServer) SubscriberPush(ctx context.Context, request *proto.SubscriberPushRequest) (*proto.SubscriberPushResponse, error) {
	for _, subscribe := range request.Subscribes {
		StoreSetOpCh <- func(ctx context.Context, conns map[string]*StoreSetConn) {
			for _, conn := range conns {
				conn.RunnerOpCh <- func(ctx context.Context, runners map[string]*SubscriberRunner) {
					runner, ok := runners[subscribe.Name]
					if !ok {
						//TODO:保存已有的订阅者
						runner = NewSubscribeRunner(subscribe, conn.conn)
						runners[subscribe.Name] = runner
						go runner.Start(ctx)
					}
					//TODO:订阅更新地址等信息
				}
			}
		}
		//TODO:待完善字段
	}
	return &proto.SubscriberPushResponse{}, nil
}

func (x *XdsServer) StoreSetPush(ctx context.Context, request *proto.StoreSetPushRequest) (*proto.StoreSetPushResponse, error) {
	var err error
	for _, store := range request.Stores {
		StoreSetOpCh <- func(ctx context.Context, conns map[string]*StoreSetConn) {
			getOrCreateConn(ctx, conns, store)
			//TODO:根据已有的 订阅, 启动runner
		}
	}
	return &proto.StoreSetPushResponse{}, err
}

func (x *XdsServer) AllocatePartition(request *proto.AllocatePartitionRequest, server proto.XdsService_AllocatePartitionServer) error {
	panic("implement me")
}

func StartXdsServer(ctx context.Context) error {
	sock, err := net.Listen("tcp", managerAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	reflection.Register(s)
	proto.RegisterXdsServiceServer(s, &XdsServer{})
	go func() {
		select {
		case <-ctx.Done():
			s.GracefulStop()
		}
	}()
	go func() {
		if err := s.Serve(sock); err != nil {
			panic(err)
		}
	}()

	return nil
}
