package storeset

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/sirupsen/logrus"
	"time"
)

type actor struct {
	in     chan func(ctx context.Context, client cloudevents.Client, target string)
	runner *SubscriberRunner
	key    string
}

func newActor(runner *SubscriberRunner, key string) *actor {
	return &actor{in: make(chan func(ctx context.Context, client cloudevents.Client, target string)), runner: runner, key: key}
}

//TODO:错误处理

func (a *actor) Start(ctx context.Context) {
	duration := a.runner.spec.Spec.Delivery.AckDuration.Duration * 10
	timer := time.NewTimer(duration)
	defer func() {
		a.runner.actorFunc <- func(actors map[string]*actor) {
			delete(actors, a.key)
		}
		if !timer.Stop() {
			<-timer.C
		}
	}()

	target := a.runner.s.GetTarget()
	cloudCtx := cloudevents.ContextWithTarget(ctx, target)
	retries := a.runner.spec.Spec.Delivery.MaxRetries
	cloudCtx = cloudevents.ContextWithRetriesExponentialBackoff(cloudCtx,
		a.runner.spec.Spec.Delivery.MaxRequestDuration.Duration,
		*retries)

	p, err := cloudevents.NewHTTP()
	if err != nil {
		logrus.Errorf("failed to create protocol: %s", err)
		return
	}

	c, err := cloudevents.NewClient(p, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		logrus.Errorf("failed to create client, %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case f := <-a.in:
			// timer may be not active, and fired
			if !timer.Stop() {
				select {
				case <-timer.C: //try to drain from the channel
				default:
				}
			}
			timer.Reset(duration)
			f(cloudCtx, c, target)
		case <-timer.C:
			return
		}
	}
}
