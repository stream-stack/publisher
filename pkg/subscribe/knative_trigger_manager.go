package subscribe

import (
	"context"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	v1 "knative.dev/eventing/pkg/apis/eventing/v1"
	externalversions2 "knative.dev/eventing/pkg/client/informers/externalversions"
	v12 "knative.dev/pkg/apis/duck/v1"
)

func knativeTriggerManager(ctx context.Context, factory externalversions2.SharedInformerFactory) {
	triggers := factory.Eventing().V1().Triggers()
	informer := triggers.Informer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			trigger, ok := obj.(*v1.Trigger)
			if !ok {
				return
			}
			AddSubscribeFn(ctx, &subscribe{
				Name:              formatSubscriptionName(trigger.Name, trigger.Namespace),
				Url:               convertSink(trigger.Spec.Subscriber),
				TargetStreamNames: formatSubscriptionName(trigger.Spec.Broker, trigger.Namespace),
				resource:          trigger,
			})
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldSub, ok := oldObj.(*v1.Trigger)
			if !ok {
				return
			}
			newSub, ok := newObj.(*v1.Trigger)
			if !ok {
				return
			}

			DelSubscribeFn(&subscribe{
				Name: formatSubscriptionName(oldSub.Name, oldSub.Namespace),
			})
			AddSubscribeFn(ctx, &subscribe{
				Name:              formatSubscriptionName(newSub.Name, newSub.Namespace),
				Url:               convertSink(newSub.Spec.Subscriber),
				TargetStreamNames: formatSubscriptionName(newSub.Spec.Broker, newSub.Namespace),
				resource:          newSub,
			})
		},
		DeleteFunc: func(obj interface{}) {
			oldSub, ok := obj.(*v1.Trigger)
			if !ok {
				return
			}
			DelSubscribeFn(&subscribe{
				Name: formatSubscriptionName(oldSub.Name, oldSub.Namespace),
			})
		},
	})
	list, err := triggers.Lister().List(labels.Everything())
	if err != nil {
		logrus.Errorf("load exist triggers error:%v", err)
		return
	}
	for _, item := range list {
		AddSubscribeFn(ctx, &subscribe{
			Name:              formatSubscriptionName(item.Name, item.Namespace),
			Url:               convertSink(item.Spec.Subscriber),
			TargetStreamNames: formatSubscriptionName(item.Spec.Broker, item.Namespace),
			resource:          item,
		})
	}
}

func convertSink(d v12.Destination) string {
	//TODO:转换url
	return d.URI.String()
}
