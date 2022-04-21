package storeset

import (
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/common/protocol/operator"
	"github.com/stream-stack/common/protocol/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/dynamic"
	"os"
	"time"
)

var subscribes = make(map[string]*operator.Subscribe)

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
	subscribe *operator.Subscribe

	client dynamic.Interface
}

func (r *SubscriberRunner) Stop() {
	r.cancelFunc()
}

func (r *SubscriberRunner) Start(ctx context.Context) {
	r.ctx, r.cancelFunc = context.WithCancel(ctx)

	eventServiceClient := store.NewEventServiceClient(r.conn)
	kvServiceClient := store.NewKVServiceClient(r.conn)

	hostname, _ := os.Hostname()
	r.subscribeName = getSubscriberName(streamName, hostname, r.subscribe.GetName())

	logrus.Infof("启动对store的分片stream订阅,hostname:%s,streamName:%s", hostname, streamName)

	var offset uint64
	go r.startAck(kvServiceClient)

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			//TODO:使用client 更新crd
			logrus.Debugf(`begin load subscribe offset key %s`, string(r.subscribeName))
			get, err := kvServiceClient.Get(ctx, &store.GetRequest{Key: r.subscribeName})
			if err != nil {
				logrus.Debugf(`load subscribe offset key %s, error:%v`, string(r.subscribeName), err)
				convert := status.Convert(err)
				if convert.Code() == codes.NotFound {
					logrus.Debugf(`load subscribe offset key %s, error is NotFound,set offset=1`, string(r.subscribeName))
					offset = 1
				} else {
					logrus.Errorf("load subscribe[%s] offset error:%v", r.subscribeName, err)
					continue
				}
			} else {
				offset = store.BytesToUint64(get.Data) + 1
				logrus.Debugf(`load subscribe offset key %s, value %d`, string(r.subscribeName), offset)
			}

			logrus.Debugf(`begin eventServiceClient subscribe`)
			subscribe, err := eventServiceClient.Subscribe(r.ctx, &store.SubscribeRequest{
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

func (r *SubscriberRunner) doRecv(subscribe store.EventService_SubscribeClient) {
	logrus.Debugf(`begin recv`)
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

func (r *SubscriberRunner) startAck(cli store.KVServiceClient) {
	logrus.Debugf(`start subscribe ack`)
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
		put, err := cli.Put(r.ctx, &store.PutRequest{
			Key: r.subscribeName,
			Val: store.Uint64ToBytes(offset),
		})
		if err != nil {
			logrus.Errorf("ack subscribe[%s] offset to [%d] error:%v,result:%+v", r.subscribeName, offset, err, put)
			continue
		}
		logrus.Debugf("ack subscribe[%s] offset to [%d]", r.subscribeName, offset)
		prev = offset
	}
}

func (r *SubscriberRunner) sendCloudEvent(recv *store.ReadResponse) error {
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
		logrus.Errorf(`unmarshal cloudevent error:%v`, err)
		return err
	}
	e.SetSubject(recv.StreamName)

	logrus.Debugf("准备发送%d 数据 %+v 至 url: %s", recv.Offset, e.String(), r.sink.Uri)
	res := c.Send(ctx, e)
	if cloudevents.IsUndelivered(res) {
		logrus.Debugf("Failed to send: %v", res)
	}
	var httpResult *cehttp.Result
	if cloudevents.ResultAs(res, &httpResult) {
		logrus.Debugf("Sent data with status code %d", httpResult.StatusCode)
	} else {
		logrus.Errorf("Send data response not http response,result:%+v", res)
	}
	logrus.Debugf("send cloudevent finish")
	return nil
}

//default-test-publisher-1-default-test
//brokerns-brokername-publisher-1-subscribens-subscribename
func getSubscriberName(name string, hostname string, getName string) []byte {
	return []byte(fmt.Sprintf("%s-%s-%s", name, hostname, getName))
}

//TODO:sink 等参数指定
func NewSubscribeRunner(sb *operator.Subscribe, conn *grpc.ClientConn, client dynamic.Interface) *SubscriberRunner {
	return &SubscriberRunner{conn: conn, subscribe: sb, sink: SubscribeSink{Uri: sb.Uri}, client: client}
}
