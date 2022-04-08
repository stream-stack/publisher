package storeset

import (
	"context"
	"fmt"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/stream-stack/publisher/pkg/proto"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestXdsServer_StoreSetPush(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.TODO())
	defer cancelFunc()

	//go StartStoreSetManager(ctx)
	//go StartXdsServer(ctx)
	//time.Sleep(time.Second * 10)

	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	dial, err := grpc.Dial("127.0.0.1:8081", grpc.WithInsecure(), grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		panic(err)
	}
	client := proto.NewXdsServiceClient(dial)
	sets := make([]*proto.StoreSet, 1)
	sets[0] = &proto.StoreSet{
		Name:      "t",
		Namespace: "ns",
		Uris:      []string{`localhost:2001`, `localhost:2002`, `localhost:2003`},
	}
	push, err := client.StoreSetPush(ctx, &proto.StoreSetPushRequest{Stores: sets})
	if err != nil {
		panic(err)
	}
	fmt.Println(push)
	subscribes := make([]*proto.Subscribe, 1)
	subscribes[0] = &proto.Subscribe{Name: "test1", Uri: "http://www.baidu.com"}
	subscriberPush, err := client.SubscriberPush(ctx, &proto.SubscriberPushRequest{Subscribes: subscribes})
	if err != nil {
		panic(err)
	}
	fmt.Println(subscriberPush)
}
