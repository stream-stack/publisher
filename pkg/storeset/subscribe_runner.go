package storeset

import (
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/sirupsen/logrus"
	v1 "github.com/stream-stack/common/crd/knative/v1"
	"github.com/stream-stack/common/protocol/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/dynamic"
	"os"
)

type SubscriberRunner struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	conn *grpc.ClientConn

	subscribeName []byte

	client     dynamic.Interface
	spec       v1.Subscription
	s          v1.Subscriber
	streamName string

	actors    map[string]*actor
	actorFunc chan func(actors map[string]*actor)

	ack *SubscriberRunnerAck

	eventServiceClient store.EventServiceClient
	kvServiceClient    store.KVServiceClient
}

func (r *SubscriberRunner) Stop() {
	r.cancelFunc()
}

func (r *SubscriberRunner) Start(ctx context.Context) {
	r.ctx, r.cancelFunc = context.WithCancel(ctx)

	hostname, _ := os.Hostname()
	r.subscribeName = getSubscriberName(r.streamName, hostname, r.s)

	logrus.Infof("启动对store的分片stream订阅,hostname:%s,streamName:%s", hostname, r.streamName)

	r.ack = NewSubscriberRunnerAck(r.kvServiceClient, r.spec.Spec.Delivery.AckDuration.Duration, r.subscribeName)
	go r.ack.Start(r.ctx)
	go r.startActorManager()
	var offset uint64

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			//TODO:使用client 更新crd
			logrus.Debugf(`begin load subscribe offset key %s`, string(r.subscribeName))
			get, err := r.kvServiceClient.Get(ctx, &store.GetRequest{Key: r.subscribeName})
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
			subscribe, err := r.eventServiceClient.Subscribe(r.ctx, &store.SubscribeRequest{
				SubscribeId: hostname,
				Regexp:      "streamName == '" + r.streamName + "'",
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

			flag := r.ack.addOffsetFlag(recv.Offset)
			key := fmt.Sprintf(`%s-%s`, recv.StreamName, recv.StreamId)
			f := func(ctx context.Context, client cloudevents.Client, target string) {
				e := cloudevents.NewEvent()
				err = e.UnmarshalJSON(recv.Data)
				if err != nil {
					logrus.Errorf(`unmarshal cloudevent error:%v`, err)
				}
				e.SetSubject(broker)

				logrus.Debugf("准备发送%d 数据 %+v 至 url: %s", recv.Offset, e.String(), target)

				res := client.Send(ctx, e)
				if cloudevents.IsUndelivered(res) {
					logrus.Debugf("Failed to send: %v", res)
					return
				}
				var httpResult *cehttp.Result
				if cloudevents.ResultAs(res, &httpResult) {
					logrus.Debugf("Sent data with status code %d", httpResult.StatusCode)
				} else {
					logrus.Errorf("Send data response not http response,result:%+v", res)
				}
				logrus.Debugf("send cloudevent finish")
				//TODO:发送失败,转到死信队列(如何判断投递成功?)

				flag.Flag = true
				r.ack.in <- recv.Offset
			}

			r.actorFunc <- func(actors map[string]*actor) {
				a, ok := actors[key]
				if !ok {
					a = newActor(r, key)
					actors[key] = a
					go a.Start(r.ctx)
				}
				a.in <- f
			}
		}
	}
}

func (r *SubscriberRunner) startActorManager() {
	r.actors = make(map[string]*actor)

	for {
		select {
		case <-r.ctx.Done():
			return
		case f := <-r.actorFunc:
			f(r.actors)
			logrus.Debugf("当前actor数量:%d", len(r.actors))
			logrus.Debugf("当前actor:%+v", r.actors)
		}
	}
}

//default-test/publisher-1/default-test
//brokerns-brokername/publisher-1/subscribens-subscribename
func getSubscriberName(name string, hostname string, s v1.Subscriber) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", name, hostname, s.String()))
}

func NewSubscribeRunner(spec v1.Subscription, s v1.Subscriber, conn *grpc.ClientConn, client dynamic.Interface, streamName string) *SubscriberRunner {
	return &SubscriberRunner{conn: conn,
		spec:               spec,
		s:                  s,
		client:             client,
		streamName:         streamName,
		eventServiceClient: store.NewEventServiceClient(conn),
		kvServiceClient:    store.NewKVServiceClient(conn),
		actorFunc:          make(chan func(actors map[string]*actor)),
	}
}
