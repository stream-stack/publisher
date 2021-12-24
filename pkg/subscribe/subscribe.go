package subscribe

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/store/pkg/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type subscribe struct {
	Name              string `json:"name"`
	Url               string `json:"url"`
	TargetStreamNames string `json:"target_stream_names"`

	cancelFunc context.CancelFunc
	ackCh      chan uint64
	dataCh     chan *protocol.ReadResponse
}

func (s *subscribe) start(ctx context.Context, eventCli protocol.EventServiceClient, kvCli protocol.KVServiceClient) {
	logrus.Printf("start subscribe[%s]", s.Name)
	ctx, s.cancelFunc = context.WithCancel(ctx)
	defer func() {
		DelSubscribeFn(s)
		s.cancelFunc()
		s.cancelFunc = nil
	}()

	err := s.startSender(ctx)
	if err != nil {
		logrus.Errorf("start subscribe[%s] sender error:%v", s.Name, err)
		return
	}
	go s.startAck(ctx, kvCli)
	for {
		var offset uint64
		get, err := kvCli.Get(ctx, &protocol.GetRequest{Key: []byte(s.Name)})
		if err != nil {
			convert := status.Convert(err)
			if convert.Code() == codes.NotFound {
				offset = 1
			} else {
				logrus.Errorf("load subscribe[%s] offset error:%v", s.Name, err)
				return
			}
		} else {
			offset = protocol.BytesToUint64(get.Data) + 1
		}
		client, err := eventCli.Subscribe(ctx, &protocol.SubscribeRequest{
			SubscribeId: s.Name,
			Regexp:      s.TargetStreamNames,
			Offset:      offset,
		})
		if err != nil {
			logrus.Errorf("call grpc subscribe[%s] error:%v", s.Name, err)
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
				recv, err := client.Recv()
				if err != nil {
					logrus.Errorf("subscribe[%s] recv data error:%v", s.Name, err)
					time.Sleep(recvDuration)
				}
				if recv != nil {
					s.dataCh <- recv
				}
			}
		}

	}
}

func (s *subscribe) stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

func (s *subscribe) startAck(ctx context.Context, cli protocol.KVServiceClient) {
	s.ackCh = make(chan uint64, 1)
	//todo:时间设置
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	var prev uint64
	var offset uint64
	for {
		select {
		case <-ctx.Done():
			close(s.ackCh)
			return
		case of := <-s.ackCh:
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
			Key: []byte(s.Name),
			Val: protocol.Uint64ToBytes(offset),
		})
		if err != nil {
			logrus.Errorf("ack subscribe[%s] offset to [%d] error:%v,result:%+v", s.Name, offset, err, put)
			continue
		}
		logrus.Printf("ack subscribe[%s] offset to [%d]", s.Name, offset)
		prev = offset
	}
}

func (s *subscribe) startSender(ctx context.Context) error {
	ctx = cloudevents.ContextWithTarget(ctx, s.Url)
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

	s.dataCh = make(chan *protocol.ReadResponse)
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(s.dataCh)
				return
			case data := <-s.dataCh:
				e := cloudevents.NewEvent()
				e.SetType(data.StreamName)
				e.SetSource(data.EventId)
				_ = e.SetData(cloudevents.ApplicationJSON, data.Data)

				logrus.Printf("准备发送%d 数据 %+v 至 url: %s", data.Offset, e.String(), s.Url)
				res := c.Send(ctx, e)
				if cloudevents.IsUndelivered(res) {
					logrus.Printf("Failed to send: %v", res)
				}
				var httpResult *cehttp.Result
				if cloudevents.ResultAs(res, &httpResult) {
					logrus.Printf("Sent data %+v with status code %d", data, httpResult.StatusCode)
					s.ackCh <- data.Offset
				} else {
					logrus.Errorf("Send data %+v response not http response", data)
				}
				logrus.Printf("发送数据完成")
			}
		}
	}()

	return nil
}
