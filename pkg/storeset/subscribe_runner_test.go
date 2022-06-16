package storeset

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/sirupsen/logrus"
	v1 "github.com/stream-stack/common/crd/knative/v1"
	"github.com/stream-stack/common/protocol/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

type fakeEventServiceClient struct {
	fakeData chan *store.ReadResponse
	data     []byte
	cancel   context.CancelFunc
}

func (f *fakeEventServiceClient) Recv() (*store.ReadResponse, error) {
	return <-f.fakeData, nil
}

func (f *fakeEventServiceClient) Header() (metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fakeEventServiceClient) Trailer() metadata.MD {
	//TODO implement me
	panic("implement me")
}

func (f *fakeEventServiceClient) CloseSend() error {
	//TODO implement me
	panic("implement me")
}

func (f *fakeEventServiceClient) Context() context.Context {
	//TODO implement me
	panic("implement me")
}

func (f *fakeEventServiceClient) SendMsg(m interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (f *fakeEventServiceClient) RecvMsg(m interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (f *fakeEventServiceClient) Apply(ctx context.Context, in *store.ApplyRequest, opts ...grpc.CallOption) (*store.ApplyResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fakeEventServiceClient) Read(ctx context.Context, in *store.ReadRequest, opts ...grpc.CallOption) (*store.ReadResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fakeEventServiceClient) Subscribe(ctx context.Context, in *store.SubscribeRequest, opts ...grpc.CallOption) (store.EventService_SubscribeClient, error) {
	logrus.Debugf("fake event service client do subscribe method")
	return f, nil
}

func (f *fakeEventServiceClient) Start(ctx context.Context) {
	go func() {
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				close(f.fakeData)
				return
			case f.fakeData <- &store.ReadResponse{
				StreamName: "a",
				StreamId:   "1",
				EventId:    uint64(i),
				Data:       f.data,
				Offset:     uint64(i),
			}:
			}
		}
		f.cancel()
	}()
	go func() {
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return
			case f.fakeData <- &store.ReadResponse{
				StreamName: "a",
				StreamId:   "2",
				EventId:    uint64(i),
				Data:       f.data,
				Offset:     uint64(i + 11),
			}:
			}
		}
	}()
}

func TestSubscriberRunner_Start(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)
	e := cloudevents.NewEvent()
	e.SetID("1")
	e.SetType("test")
	e.SetSource("test")
	e.SetData(cloudevents.ApplicationJSON, []byte("test"))
	json, err := e.MarshalJSON()
	if err != nil {
		t.Error(err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	retry := 5
	subscription := v1.Subscription{
		Spec: v1.SubscriptionSpec{
			Broker: "",
			Delivery: &v1.SubscriberDelivery{
				MaxRequestDuration: &metav1.Duration{
					Duration: time.Second * 5,
				},
				MaxRetries:  &retry,
				AckDuration: &metav1.Duration{Duration: time.Second * 5},
			},
			Filter: nil,
			Subscriber: v1.Subscriber{
				Uri: &v1.SubscriberUri{
					Uri:      "www.baidu.com",
					Protocol: "http",
				},
			},
		},
	}
	runner := NewSubscribeRunner(subscription, subscription.Spec.Subscriber, nil, nil, "a")
	f := &fakeEventServiceClient{fakeData: make(chan *store.ReadResponse, 1), data: json, cancel: cancelFunc}
	f.Start(ctx)

	runner.kvServiceClient = &fakeKVServiceClient{}
	runner.eventServiceClient = f
	time.Sleep(time.Second)
	go runner.Start(ctx)
	select {
	case <-ctx.Done():
		fmt.Println("context done,exit unit test")
	}
}
