package subscribe

import (
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/publisher/pkg/config"
	"github.com/stream-stack/store/pkg/protocol"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"time"
)

var eventServiceClient protocol.EventServiceClient
var kvServiceClient protocol.KVServiceClient
var subscribes = make(map[string]*subscribe)

func AddSubscribeFn(ctx context.Context, sub *subscribe) {
	OperationCh <- func(m map[string]*subscribe) {
		m[sub.Name] = sub
		go sub.start(ctx, eventServiceClient, kvServiceClient)
	}
}
func DelSubscribeFn(sub *subscribe) {
	OperationCh <- func(m map[string]*subscribe) {
		s, ok := m[sub.Name]
		if !ok {
			return
		}
		s.stop()
	}
}

type Operation func(map[string]*subscribe)

var OperationCh = make(chan Operation)

func StartManager(ctx context.Context) error {
	serviceConfig := `{"healthCheckConfig": {"serviceName": ""}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	conn, err := grpc.Dial(fmt.Sprintf(`multi:///%s`, config.Address),
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		logrus.Errorf("dialing failed: %v", err)
		return err
	}
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		}
	}()
	eventServiceClient = protocol.NewEventServiceClient(conn)
	kvServiceClient = protocol.NewKVServiceClient(conn)

	//loadStatic
	go startSubscribesManager(ctx)
	if err = loadStaticSubscribe(ctx); err != nil {
		return err
	}

	return nil
}

func loadStaticSubscribe(ctx context.Context) error {
	//AddSubscribeFn()
	AddSubscribeFn(ctx, &subscribe{
		Name: "test",
		Url:  "http://localhost:8080",
		//TargetStreamNames: "*",
	})
	//DelSubscribeFn()
	return nil
}

func startSubscribesManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case fn := <-OperationCh:
			fn(subscribes)
		}
	}
}
