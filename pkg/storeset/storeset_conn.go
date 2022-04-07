package storeset

import (
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/publisher/pkg/proto"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"strings"
	"time"
)

type StoreSetConn struct {
	conn *grpc.ClientConn
	name string
	uris []string

	runners    map[string]*SubscriberRunner
	RunnerOpCh chan func(ctx context.Context, runners map[string]*SubscriberRunner)

	ctx        context.Context
	cancelFunc context.CancelFunc
}

func (c *StoreSetConn) connect() error {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "store"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	target := "multi:///" + strings.Join(c.uris, ",")
	conn, err := grpc.Dial(target,
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		logrus.Errorf("连接%s出现错误:%v", target, err)
		return err
	}
	c.conn = conn
	//TODO:加入健康检查
	return nil
}

func getOrCreateConn(ctx context.Context, conns map[string]*StoreSetConn, store *proto.StoreSet) *StoreSetConn {
	name := formatStoreName(store)
	conn, ok := conns[name]
	if ok {
		return conn
	}
	conn = &StoreSetConn{
		name:    name,
		uris:    store.Uris,
		runners: make(map[string]*SubscriberRunner),
	}
	go conn.Start(ctx)
	storesetConns[name] = conn
	return conn
}

func formatStoreName(store *proto.StoreSet) string {
	return fmt.Sprintf("%s/%s", store.Namespace, store.Name)
}

func (c *StoreSetConn) Stop() {
	c.cancelFunc()
	c.conn.Close()
}

func (c *StoreSetConn) Start(ctx context.Context) {
	c.ctx, c.cancelFunc = context.WithCancel(ctx)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			err := c.connect()
			if err != nil {
				//间隔5秒重连
				time.Sleep(connectionRetryDuration)
				continue
			}
			c.doWork(c.ctx)
		}
	}
}

func (c *StoreSetConn) doWork(ctx context.Context) {
	c.RunnerOpCh = make(chan func(ctx context.Context, runners map[string]*SubscriberRunner), 1)
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-c.RunnerOpCh:
			op(c.ctx, c.runners)
		}
	}
}
