package subscribe

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	v1 "knative.dev/eventing/pkg/apis/messaging/v1"
	externalversions2 "knative.dev/eventing/pkg/client/informers/externalversions"
	v12 "knative.dev/pkg/apis/duck/v1"
)

func knativeSubscribeManager(ctx context.Context, factory externalversions2.SharedInformerFactory) {
	subscriptions := factory.Messaging().V1().Subscriptions()
	informer := subscriptions.Informer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			subscription, ok := obj.(*v1.Subscription)
			if !ok {
				return
			}
			AddSubscribeFn(ctx, &subscribe{
				Name:              formatSubscriptionName(subscription.Name, subscription.Namespace),
				Url:               convertUrl(subscription.Spec.Subscriber),
				TargetStreamNames: subscription.Spec.Channel.Name,
				resource:          subscription,
			})
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldSub, ok := oldObj.(*v1.Subscription)
			if !ok {
				return
			}
			newSub, ok := newObj.(*v1.Subscription)
			if !ok {
				return
			}

			DelSubscribeFn(&subscribe{
				Name: formatSubscriptionName(oldSub.Name, oldSub.Namespace),
			})
			AddSubscribeFn(ctx, &subscribe{
				Name:              formatSubscriptionName(newSub.Name, newSub.Namespace),
				Url:               convertUrl(newSub.Spec.Subscriber),
				TargetStreamNames: formatSubscriptionName(newSub.Spec.Channel.Name, newSub.Spec.Channel.Namespace),
				resource:          newSub,
			})
		},
		DeleteFunc: func(obj interface{}) {
			oldSub, ok := obj.(*v1.Subscription)
			if !ok {
				return
			}
			DelSubscribeFn(&subscribe{
				Name: formatSubscriptionName(oldSub.Name, oldSub.Namespace),
			})
		},
	})
	list, err := subscriptions.Lister().List(labels.Everything())
	if err != nil {
		logrus.Errorf("load exist subscribe error:%v", err)
		return
	}
	for _, item := range list {
		AddSubscribeFn(ctx, &subscribe{
			Name:              formatSubscriptionName(item.Name, item.Namespace),
			Url:               convertUrl(item.Spec.Subscriber),
			TargetStreamNames: formatSubscriptionName(item.Spec.Channel.Name, item.Spec.Channel.Namespace),
			resource:          item,
		})
	}
}

func formatSubscriptionName(name, namespace string) string {
	return fmt.Sprintf("%s/%s", name, namespace)
}

func convertUrl(subscriber *v12.Destination) string {
	//todo:解析结果
	return subscriber.URI.String()
}
