package subscribe

import (
	"context"
	_ "github.com/Jille/grpc-multi-resolver"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/publisher/pkg/config"
	"github.com/stream-stack/store/pkg/protocol"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"k8s.io/client-go/kubernetes/scheme"
	v1 "k8s.io/client-go/kubernetes/typed/events/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	v11 "knative.dev/eventing/pkg/client/clientset/versioned"
	externalversions2 "knative.dev/eventing/pkg/client/informers/externalversions"
	"time"
)

var eventServiceClient protocol.EventServiceClient
var kvServiceClient protocol.KVServiceClient
var eventRecord events.EventRecorder
var subscribes = make(map[string]*subscribe)

func AddSubscribeFn(ctx context.Context, sub *subscribe) {
	OperationCh <- func(m map[string]*subscribe) {
		m[sub.Name] = sub
		go sub.start(ctx)
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
	serviceConfig := `{"healthCheckConfig": {"serviceName": "store"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	conn, err := grpc.Dial(config.Address,
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

	err = startKnativeSourceManagers(ctx, knativeSubscribeManager, knativeTriggerManager)
	if err != nil {
		logrus.Errorf("start knative source manager error:%v", err)
		return err
	}
	if err = loadStaticSubscribe(ctx); err != nil {
		return err
	}

	return nil
}

type knativeResourceManager func(ctx context.Context, factory externalversions2.SharedInformerFactory)

func startKnativeSourceManagers(ctx context.Context, managers ...knativeResourceManager) error {
	inClusterConfig, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := v11.NewForConfig(inClusterConfig)
	if err != nil {
		return err
	}

	//NewBroadcaster
	v1Client := v1.New(client.RESTClient())
	broadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: v1Client})
	eventRecord = broadcaster.NewRecorder(scheme.Scheme, "publisher")

	factory := externalversions2.NewSharedInformerFactoryWithOptions(client, time.Minute)
	for _, manager := range managers {
		manager(ctx, factory)
	}
	c := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c <- struct{}{}
		}
	}()
	go factory.Start(c)

	factory.WaitForCacheSync(c)
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
			close(OperationCh)
			return
		case fn := <-OperationCh:
			fn(subscribes)
		}
	}
}
