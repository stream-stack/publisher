package storeset

import (
	"container/list"
	"context"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/common/protocol/store"
	"time"
)

type SubscriberRunnerAck struct {
	kvClient        store.KVServiceClient
	in              chan uint64
	addAckActorFunc chan func(queue *list.List)
	syncDuration    time.Duration
	subscribeName   []byte
}

type ackFlag struct {
	offset uint64
	Flag   bool
}

func (a *SubscriberRunnerAck) addOffsetFlag(offset uint64) *ackFlag {
	logrus.Debugf(`add ack flag,offset:%d`, offset)
	flag := &ackFlag{offset: offset, Flag: false}
	a.addAckActorFunc <- func(queue *list.List) {
		queue.PushBack(flag)
		logrus.Debugf("push back ack flag,queue len:%d", queue.Len())
	}
	return flag
}

func (a *SubscriberRunnerAck) Start(ctx context.Context) {
	//a.in = make(chan uint64, 1)
	//a.addAckActorFunc = make(chan func(queue *list.List))
	logrus.Debugf(`start subscribe ack`)
	ackActorQueue := list.New()

	ticker := time.NewTicker(a.syncDuration)
	defer ticker.Stop()

	var prev uint64
	var offset uint64
	for {
		select {
		case <-ctx.Done():
			close(a.in)
			return
		case f := <-a.addAckActorFunc:
			f(ackActorQueue)
			logrus.Debugf("add ack actor, queue len: %d", ackActorQueue.Len())
		case of := <-a.in: //读取到ack,不直接更新offset,而是比较ack和offset,如果相等,则不更新offset
			logrus.Debugf(`ack recv offset:%d,currentOffset:%d,queueLen:%d`, of, offset, ackActorQueue.Len())
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
			logrus.Debugf(`计算后 offset:%d,currentOffset:%d,queueLen:%d`, of, offset, ackActorQueue.Len())

			if prev == 0 {
				prev = of
			}
			continue
		case <-ticker.C: //每隔一段时间更新offset

		}
		if offset <= 0 || offset == prev {
			continue
		}
		put, err := a.kvClient.Put(ctx, &store.PutRequest{
			Key: a.subscribeName,
			Val: store.Uint64ToBytes(offset),
		})
		if err != nil {
			logrus.Errorf("ack subscribe[%s] offset to [%d] error:%v,result:%+v", a.subscribeName, offset, err, put)
			continue
		}
		logrus.Debugf("ack subscribe[%s] offset to [%d]", a.subscribeName, offset)
		prev = offset
	}
}

func NewSubscriberRunnerAck(kvClient store.KVServiceClient, syncDuration time.Duration, subscribeName []byte) *SubscriberRunnerAck {
	return &SubscriberRunnerAck{
		kvClient:        kvClient,
		syncDuration:    syncDuration,
		subscribeName:   subscribeName,
		in:              make(chan uint64, 1),
		addAckActorFunc: make(chan func(queue *list.List)),
	}
}
