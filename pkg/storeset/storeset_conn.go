package storeset

import (
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/common/protocol/operator"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"k8s.io/client-go/dynamic"
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
	client     dynamic.Interface
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

func getOrCreateConn(ctx context.Context, conns map[string]*StoreSetConn, store *operator.StoreSet, client dynamic.Interface) *StoreSetConn {
	name := formatStoreName(store)
	conn, ok := conns[name]
	if ok {
		logrus.Debugf(`connection exist,return`)
		return conn
	}
	conn = &StoreSetConn{
		name:       name,
		uris:       store.Uris,
		runners:    make(map[string]*SubscriberRunner),
		client:     client,
		RunnerOpCh: make(chan func(ctx context.Context, runners map[string]*SubscriberRunner), 1),
	}
	go conn.Start(ctx)
	storesetConns[name] = conn
	return conn
}

func formatStoreName(store *operator.StoreSet) string {
	return fmt.Sprintf("%s/%s", store.Namespace, store.Name)
}

func (c *StoreSetConn) Stop() {
	c.cancelFunc()
	c.conn.Close()
}

func (c *StoreSetConn) Start(ctx context.Context) {
	logrus.Debugf(`start storeset connection %s,uris: %v`, c.name, c.uris)
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
			c.startExistRunner(ctx)
			c.doWork(c.ctx)
		}
	}
}

func (c *StoreSetConn) doWork(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-c.RunnerOpCh:
			op(c.ctx, c.runners)
		}
	}
}

func (c *StoreSetConn) startExistRunner(ctx context.Context) {
	for s, subscribe := range subscribes {
		runner, ok := c.runners[s]
		logrus.Debugf(`start exist subscribe %s for connection %s`, s, c.name)
		if !ok {
			logrus.Debugf(`subscribe %s not exist for connection %s, create and start`, s, c.name)
			runner = NewSubscribeRunner(subscribe, subscribe.Spec.Subscriber, c.conn, c.client, streamName)
			c.runners[subscribe.Name] = runner
			go runner.Start(ctx)
		}
	}
}
