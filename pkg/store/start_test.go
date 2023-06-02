package store

import (
	"context"
	"github.com/gogo/protobuf/sortkeys"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/common/util"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestSortUint64(t *testing.T) {
	uint64s := make([]uint64, 0)
	uint64s = append(uint64s, 1)
	uint64s = append(uint64s, 3)
	uint64s = append(uint64s, 2)
	uint64s = append(uint64s, 5)
	uint64s = append(uint64s, 4)
	sortkeys.Uint64s(uint64s)
	t.Log(uint64s)
}

type fakeClient struct {
	c chan interface{}
}

func (f *fakeClient) Get(ctx context.Context, in *v1.GetRequest, opts ...grpc.CallOption) (*v1.GetResponse, error) {
	return &v1.GetResponse{
		Value: util.Uint64ToBytes(1),
	}, nil
}

func (f *fakeClient) Put(ctx context.Context, in *v1.PutRequest, opts ...grpc.CallOption) (*v1.PutResponse, error) {
	toUint64 := util.BytesToUint64(in.Value)
	logrus.Debugf("put key:%s, value:%v", in.Key, toUint64)
	if toUint64 == 5 {
		close(f.c)
	}
	return &v1.PutResponse{}, nil
}

func TestOffsetSyncer(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)
	todo := context.TODO()
	viper.SetDefault("OffsetSyncInterval", time.Second*1)
	c := make(chan interface{})
	kc := &fakeClient{c: c}
	_, offsetCh, err := startOffsetSyncer(todo, kc, "")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		offsetCh <- 2
		offsetCh <- 5
		offsetCh <- 4
		offsetCh <- 3
	}()
	<-c
}
