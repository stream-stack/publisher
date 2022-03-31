package storeset

import (
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/publisher/pkg/proto"
	"github.com/stream-stack/store/pkg/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/status"
	"os"
	"time"
)

type Subscribe struct {
	name string
}

type SubscriberRunner struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	storeName  string
	uris       []string
	conn       *grpc.ClientConn
	subscribeName []byte
	ackCh         chan uint64
	clean         func()
}

func (r *SubscriberRunner) Stop() {
	r.clean()
	r.cancelFunc()
}

func (r *SubscriberRunner) Start(ctx context.Context) {
	var err error
	r.ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.Stop()

	eventServiceClient := protocol.NewEventServiceClient(r.conn)
	kvServiceClient := protocol.NewKVServiceClient(r.conn)

	hostname, _ := os.Hostname()
	streamName := os.Getenv("STREAM_NAME")
	logrus.Infof("启动对store的分片stream订阅,hostname:%s,streamName:%s", hostname, streamName)

	var offset uint64
	r.subscribeName = getSubscriberName(streamName, hostname)
	get, err := kvServiceClient.Get(ctx, &protocol.GetRequest{Key: r.subscribeName})
	if err != nil {
		convert := status.Convert(err)
		if convert.Code() == codes.NotFound {
			offset = 1
		} else {
			logrus.Errorf("load subscribe[%s] offset error:%v", r.subscribeName, err)
			return
		}
	} else {
		offset = protocol.BytesToUint64(get.Data) + 1
	}

	subscribe, err := eventServiceClient.Subscribe(r.ctx, &protocol.SubscribeRequest{
		SubscribeId: hostname,
		Regexp:      "streamName == '" + streamName + "'",
		Offset:      offset,
	})
	if err != nil {
		logrus.Warnf("订阅partition出错,%v", err)
		return
	}
	go r.startAck(r.ctx, kvServiceClient)

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			recv, err := subscribe.Recv()
			if err != nil {
				logrus.Warnf("接收消息出错,%v", err)
				continue
			}

			logrus.Debugf("收到消息,%+v", recv)
			//TODO:发送event

			r.ackCh <- recv.Offset
		}
	}

}

func (r *SubscriberRunner) startAck(ctx context.Context, cli protocol.KVServiceClient) {
	r.ackCh = make(chan uint64, 1)
	ticker := time.NewTicker(ackDuration)
	defer ticker.Stop()
	var prev uint64
	var offset uint64
	for {
		select {
		case <-ctx.Done():
			close(r.ackCh)
			return
		case of := <-r.ackCh:
			offset = of
			if prev == 0 {
				prev = of
			}
			continue
		case <-ticker.C:

		}
		if offset <= 0 || offset == prev {
			continue
		}
		put, err := cli.Put(ctx, &protocol.PutRequest{
			Key: r.subscribeName,
			Val: protocol.Uint64ToBytes(offset),
		})
		if err != nil {
			logrus.Errorf("ack subscribe[%s] offset to [%d] error:%v,result:%+v", r.subscribeName, offset, err, put)
			continue
		}
		logrus.Printf("ack subscribe[%s] offset to [%d]", r.subscribeName, offset)
		prev = offset
	}
}

func getSubscriberName(name string, hostname string) []byte {
	return []byte(fmt.Sprintf("%s-%s", name, hostname))
}

func formatStoreName(store *proto.StoreSet) string {
	return fmt.Sprintf("%s/%s", store.Namespace, store.Name)
}

func NewSubscribeRunner(name string, conn *grpc.ClientConn, f func()) *SubscriberRunner {
	return &SubscriberRunner{storeName: name, conn: conn, clean: f}
}
