package storeset

import (
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
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

type SubscribeSink struct {
	Uri string
}

type SubscriberRunner struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	conn *grpc.ClientConn

	subscribeName []byte
	ackCh         chan uint64

	sink      SubscribeSink
	subscribe *proto.Subscribe
}

func (r *SubscriberRunner) Stop() {
	r.cancelFunc()
}

func (r *SubscriberRunner) Start(ctx context.Context) {
	r.ctx, r.cancelFunc = context.WithCancel(ctx)

	eventServiceClient := protocol.NewEventServiceClient(r.conn)
	kvServiceClient := protocol.NewKVServiceClient(r.conn)

	hostname, _ := os.Hostname()
	streamName := os.Getenv("STREAM_NAME")
	r.subscribeName = getSubscriberName(streamName, hostname, r.subscribe.GetName())

	logrus.Infof("启动对store的分片stream订阅,hostname:%s,streamName:%s", hostname, streamName)

	var offset uint64
	go r.startAck(kvServiceClient)

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			get, err := kvServiceClient.Get(ctx, &protocol.GetRequest{Key: r.subscribeName})
			if err != nil {
				convert := status.Convert(err)
				if convert.Code() == codes.NotFound {
					offset = 1
				} else {
					logrus.Errorf("load subscribe[%s] offset error:%v", r.subscribeName, err)
					continue
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
				continue
			}

			r.doRecv(subscribe)
		}
	}

}

func (r *SubscriberRunner) doRecv(subscribe protocol.EventService_SubscribeClient) {
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

			if err = r.sendCloudEvent(recv); err != nil {
				logrus.Errorf(`发送 cloudevent出现错误`)
			}

			r.ackCh <- recv.Offset
		}
	}
}

func (r *SubscriberRunner) startAck(cli protocol.KVServiceClient) {
	r.ackCh = make(chan uint64, 1)
	ticker := time.NewTicker(ackDuration)
	defer ticker.Stop()

	var prev uint64
	var offset uint64
	for {
		select {
		case <-r.ctx.Done():
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
		put, err := cli.Put(r.ctx, &protocol.PutRequest{
			Key: r.subscribeName,
			Val: protocol.Uint64ToBytes(offset),
		})
		if err != nil {
			logrus.Errorf("ack subscribe[%s] offset to [%d] error:%v,result:%+v", r.subscribeName, offset, err, put)
			continue
		}
		logrus.Debugf("ack subscribe[%s] offset to [%d]", r.subscribeName, offset)
		prev = offset
	}
}

func (r *SubscriberRunner) sendCloudEvent(recv *protocol.ReadResponse) error {
	ctx := cloudevents.ContextWithTarget(r.ctx, r.sink.Uri)
	ctx = cloudevents.ContextWithRetriesExponentialBackoff(ctx, senderMaxRequestDuration, senderMaxRequestRetryCount)

	p, err := cloudevents.NewHTTP()
	if err != nil {
		logrus.Errorf("failed to create protocol: %s", err)
		return err
	}

	c, err := cloudevents.NewClient(p, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		logrus.Errorf("failed to create client, %v", err)
		return err
	}

	e := cloudevents.NewEvent()
	err = e.UnmarshalJSON(recv.Data)
	if err != nil {
		return err
	}
	//e.SetID(strconv.FormatUint(recv.EventId,10))
	//e.SetType(recv.StreamName)
	//e.SetSubject(recv.StreamName)
	//e.SetSource(recv.StreamId)
	//_ = e.SetData(cloudevents.ApplicationJSON, recv.Data)

	logrus.Debugf("准备发送%d 数据 %+v 至 url: %s", recv.Offset, e.String(), r.sink.Uri)
	res := c.Send(ctx, e)
	if cloudevents.IsUndelivered(res) {
		logrus.Debugf("Failed to send: %v", res)
	}
	var httpResult *cehttp.Result
	if cloudevents.ResultAs(res, &httpResult) {
		logrus.Debugf("Sent data with status code %d", httpResult.StatusCode)
	} else {
		logrus.Errorf("Send data response not http response")
	}
	logrus.Debugf("发送数据完成")
	return nil
}

//default-test-publisher-1-default-test
//brokerns-brokername-publisher-1-subscribens-subscribename
func getSubscriberName(name string, hostname string, getName string) []byte {
	return []byte(fmt.Sprintf("%s-%s-%s", name, hostname, getName))
}

//TODO:sink 等参数指定
func NewSubscribeRunner(sb *proto.Subscribe, conn *grpc.ClientConn) *SubscriberRunner {
	return &SubscriberRunner{conn: conn, subscribe: sb}
}
