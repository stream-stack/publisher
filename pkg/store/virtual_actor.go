package store

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/common/util"
	"time"
)

var request = make(chan func(actors map[string]*virtualActor))

type actorData struct {
	e *v1.CloudEvent
	c uint
}

type virtualActor struct {
	signal chan *v1.CloudEvent
	key    string
	events []*actorData
	total  uint
}

func (a *virtualActor) start(ctx context.Context) {
	a.signal = make(chan *v1.CloudEvent)
	a.events = make([]*actorData, 0)
	a.total = 3
	lifetime := time.NewTimer(virtualActorLifetime)

	go func() {
		defer a.close()
		defer lifetime.Stop()
		for {
			select {
			case <-ctx.Done():
				logrus.Debugf("[virtual-actor][%s]context done,will be closed", a.key)
				return
			case <-lifetime.C:
				logrus.Debugf("[virtual-actor][%s]virtual actor timeout,will be closed", a.key)
				return
			case ev := <-a.signal:
				lifetime.Reset(virtualActorLifetime)
				//判断接收到的event是否达到共识要求(有超过半数),调用sender发送
				if len(a.events) == 0 {
					a.events = append(a.events, &actorData{e: ev, c: 1})
					continue
				}
				for _, e := range a.events {
					if util.EventEqual(ev, e.e) {
						e.c++
						if e.c > a.total/2 {
							logrus.Debugf("[virtual-actor][%s]reach consensus", a.key)
							sendCloudEventWithRetry(ctx, ev)
						}
					}
				}
			}
		}
	}()
}

func sendCloudEvent(cnx context.Context, ev *v1.CloudEvent) error {
	c, err := cloudevents.NewClientHTTP()
	if err != nil {
		logrus.Errorf("[cloudevent-client]failed to create client, %v", err)
		return err
	}
	event := cloudevents.NewEvent()
	event.SetID(ev.Id)
	event.SetSource(ev.Source)
	event.SetType(ev.Type)
	err = event.SetData(cloudevents.ApplicationJSON, string(ev.GetBinaryData()))
	if err != nil {
		logrus.Errorf("[cloudevent-client]set cloudevent data error:%v", err)
		return err
	}

	ctx := cloudevents.ContextWithTarget(cnx, subscribeUrl)

	if result := c.Send(ctx, event); cloudevents.IsUndelivered(result) {
		logrus.Errorf("[cloudevent-client]failed to send cloudevent: %v", result)
		return result
	} else {
		logrus.Debugf("[cloudevent-client]cloudevent sent, accepted: %t", cloudevents.IsACK(result))
	}
	return nil
}

func sendCloudEventWithRetry(cnx context.Context, ev *v1.CloudEvent) {
	var i int
	//Exponential backoff retry
	for err := sendCloudEvent(cnx, ev); err != nil; i++ {
		duration := time.Millisecond * 100 * time.Duration(i)
		logrus.Errorf("[cloudevent-client]send cloudevent error:%v,wait %v retry send", err, duration)
		if i > 50 {
			i = 0
		}
		time.Sleep(duration)
	}
}

func (a *virtualActor) close() {
	request <- func(actors map[string]*virtualActor) {
		delete(actors, a.key)
	}
}

var actors = make(map[string]*virtualActor)
var virtualActorLifetime = time.Second * 10
var subscribeUrl string

func StartConsensus(ctx context.Context) {
	virtualActorLifetime = viper.GetDuration("virtual-actor-lifetime")
	subscribeUrl = viper.GetString("subscribe-url")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case f := <-request:
				f(actors)
			}
		}
	}()
}

func BeginConsensus(ctx context.Context, ev *v1.CloudEvent) {
	key := getKey(ev)
	request <- func(actors map[string]*virtualActor) {
		actor, ok := actors[key]
		logrus.Debugf("[virtual-actor][%s]begin consensus", key)
		if !ok {
			actor = &virtualActor{key: key}
			actor.start(ctx)
			actors[key] = actor
		}
		actor.signal <- ev
	}
}

func getKey(ev *v1.CloudEvent) string {
	return fmt.Sprintf("%s/%s", ev.Source, ev.Id)
}
