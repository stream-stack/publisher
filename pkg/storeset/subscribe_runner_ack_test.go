package storeset

import (
	"container/list"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/common/protocol/store"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestQueueRemove(t *testing.T) {
	ackActorQueue := list.New()
	ackActorQueue.PushBack(&ackFlag{Flag: true, offset: 1})
	ackActorQueue.PushBack(&ackFlag{Flag: true, offset: 2})
	ackActorQueue.PushBack(&ackFlag{Flag: true, offset: 3})
	v := &ackFlag{Flag: false, offset: 4}
	ackActorQueue.PushBack(v)
	ackActorQueue.PushBack(&ackFlag{Flag: false, offset: 5})
	ackActorQueue.PushBack(&ackFlag{Flag: true, offset: 6})
	var offset uint64

	for e := ackActorQueue.Front(); e != nil; e = ackActorQueue.Front() {
		//if flag is true , remove from queue
		flag := e.Value.(*ackFlag)
		if flag.Flag {
			offset = flag.offset
			ackActorQueue.Remove(e)
		} else {
			break
		}
	}

	fmt.Println("offset:", offset, "queueLen:", ackActorQueue.Len())
	v.Flag = true

	for e := ackActorQueue.Front(); e != nil; e = ackActorQueue.Front() {
		//if flag is true , remove from queue
		flag := e.Value.(*ackFlag)
		if flag.Flag {
			offset = flag.offset
			ackActorQueue.Remove(e)
		} else {
			break
		}
	}

	fmt.Println("offset:", offset, "queueLen:", ackActorQueue.Len())
}

type fakeKVServiceClient struct {
}

func (f *fakeKVServiceClient) Put(ctx context.Context, in *store.PutRequest, opts ...grpc.CallOption) (*store.PutResponse, error) {
	fmt.Println("fake kv service client do put method")
	fmt.Printf("%+v\n", in)
	return &store.PutResponse{
		Ack:     true,
		Message: "",
	}, nil
}

func (f *fakeKVServiceClient) Get(ctx context.Context, in *store.GetRequest, opts ...grpc.CallOption) (*store.GetResponse, error) {
	fmt.Println("fake kv service client do get method")
	return &store.GetResponse{
		Data: store.Uint64ToBytes(1),
	}, nil
}

func (f *fakeKVServiceClient) GetRange(ctx context.Context, in *store.GetRequest, opts ...grpc.CallOption) (*store.GetRangeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func TestSubscriberRunnerAck_Start(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)
	ack := NewSubscriberRunnerAck(&fakeKVServiceClient{}, time.Second, []byte("test"))
	ctx, cancelFunc := context.WithCancel(context.TODO())
	go ack.Start(ctx)

	offsetFlag1 := ack.addOffsetFlag(1)
	offsetFlag2 := ack.addOffsetFlag(5)
	offsetFlag3 := ack.addOffsetFlag(7)
	offsetFlag5 := ack.addOffsetFlag(9)
	offsetFlag4 := ack.addOffsetFlag(12)
	go func() {
		time.Sleep(time.Second * 1)
		offsetFlag1.Flag = true
		ack.in <- 1
	}()
	go func() {
		time.Sleep(time.Second * 2)
		offsetFlag2.Flag = true
		ack.in <- 5
	}()
	go func() {
		time.Sleep(time.Second * 3)
		offsetFlag3.Flag = true
		ack.in <- 7
	}()

	go func() {
		time.Sleep(time.Second * 4)
		offsetFlag4.Flag = true
		ack.in <- 12
	}()
	go func() {
		time.Sleep(time.Second * 8)
		offsetFlag5.Flag = true
		ack.in <- 9
	}()

	time.Sleep(time.Second * 10)
	cancelFunc()
}
