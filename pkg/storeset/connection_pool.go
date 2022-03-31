package storeset

import (
	"context"
	_ "github.com/Jille/grpc-multi-resolver"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"strings"
	"time"
)

type AddStoreSet struct {
	name string
	uris []string
}

var AddStoreSetCh = make(chan AddStoreSet)
var RemoveStoreSetCh = make(chan string)

type StoreSetConn struct {
	conn    *grpc.ClientConn
	uris    []string
	name    string
	runners map[string]*SubscriberRunner

	addSubscribeCh    chan Subscribe
	removeSubscribeCh chan string
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

var storesetConns = make(map[string]*StoreSetConn)

func NewStoreSetConn(store AddStoreSet) *StoreSetConn {
	return &StoreSetConn{
		name:              store.name,
		uris:              store.uris,
		addSubscribeCh:    make(chan Subscribe, 1),
		removeSubscribeCh: make(chan string, 1),
		runners:           make(map[string]*SubscriberRunner),
	}
}

func (c *StoreSetConn) Start(ctx context.Context) {
	defer func() {
		RemoveStoreSetCh <- c.name
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := c.connect()
			if err != nil {
				//间隔5秒重连
				time.Sleep(connectionRetryDuration)
				continue
			}
			for {
				select {
				case <-ctx.Done():
					return
				case add := <-c.addSubscribeCh:
					runner, ok := c.runners[add.name]
					if !ok {
						runner = NewSubscribeRunner(add.name, c.conn, func() {
							delete(c.runners, add.name)
						})
						c.runners[add.name] = runner
						runner.Start(ctx)
					}
				case rm := <-RemoveStoreSetCh:
					runner, ok := c.runners[rm]
					if !ok {
						continue
					}
					runner.Stop()
				}
			}
		}
	}
}
