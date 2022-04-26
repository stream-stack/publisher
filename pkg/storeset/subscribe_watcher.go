package storeset

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	v1 "github.com/stream-stack/common/crd/knative/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"os"
	"time"
)

func init() {
	watchers = append(watchers, watchSubscribe)
}

var subscribes = make(map[string]v1.Subscription)

func watchSubscribe(ctx context.Context, client dynamic.Interface) error {
	namespace := os.Getenv("NAMESPACE")

	gvr := schema.GroupVersionResource{Group: "knative.stream-stack.tanx", Version: "v1", Resource: "subscriptions"}
	informerFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, time.Hour, namespace, func(options *metav1.ListOptions) {
	})
	resource := informerFactory.ForResource(gvr)
	handler := &subscribeResourceEventHandler{ctx: ctx, client: client}
	resource.Informer().AddEventHandler(handler)

	logrus.Debugf("start informer at namespace [%s] for %s/%s/%s", namespace, gvr.Group, gvr.Version, gvr.Resource)
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())
	return nil
}

type subscribeResourceEventHandler struct {
	ctx    context.Context
	client dynamic.Interface
}

func (s *subscribeResourceEventHandler) OnAdd(obj interface{}) {
	sbs, err := convertSubscribe(obj)
	if err != nil {
		logrus.Errorf("convertSubscribe k8s subscribe(crd) to subscribes error:%v", err)
		return
	}
	if sbs == nil {
		logrus.Debugf("convertSubscribe k8s subscribe(crd) to subscribes is nil")
		return
	}
	if sbs.Spec.Broker != broker {
		logrus.Debugf("OnAdd, subscription(crd).spec.broker %s is not for this broker(%s), skip", sbs.Spec.Broker, broker)
		return
	}
	logrus.Debugf("received subscribes %v add", sbs.Name)
	StoreSetOpCh <- func(ctx context.Context, conns map[string]*StoreSetConn) {
		subscription := *sbs
		subscribes[sbs.Name] = subscription
		for _, conn := range conns {
			conn.RunnerOpCh <- func(ctx context.Context, runners map[string]*SubscriberRunner) {
				runner, ok := runners[sbs.Name]
				if !ok {
					runner = NewSubscribeRunner(subscription, subscription.Spec.Subscriber, conn.conn, s.client, streamName)
					runners[sbs.Name] = runner
					go runner.Start(ctx)
				}
				//TODO:订阅更新地址等信息
			}
		}
	}
}

func (s *subscribeResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	//不管怎么更新,sts的svc url地址不会改变
	logrus.Debugf("unsupport subscribe update,ignore")
}

func (s *subscribeResourceEventHandler) OnDelete(obj interface{}) {
	sbs, err := convertSubscribe(obj)
	if err != nil {
		logrus.Errorf("convertSubscribe k8s subscribe(crd) to subscribes error:%v", err)
		return
	}
	if sbs == nil {
		logrus.Debugf("convertSubscribe k8s subscribe(crd) to subscribes is nil")
		return
	}
	if sbs.Spec.Broker != broker {
		logrus.Debugf("OnDelete, subscription(crd).spec.broker %s is not for this broker(%s), skip", sbs.Spec.Broker, broker)
		return
	}
	logrus.Debugf("received subscribe %v delete", sbs.Name)
	StoreSetOpCh <- func(ctx context.Context, conns map[string]*StoreSetConn) {
		subscription := *sbs
		subscribes[sbs.Name] = subscription
		for _, conn := range conns {
			conn.RunnerOpCh <- func(ctx context.Context, runners map[string]*SubscriberRunner) {
				runner, ok := runners[subscription.Name]
				if ok {
					runner.Stop()
				}
				delete(subscribes, subscription.Name)
				delete(runners, subscription.Name)
			}
		}
	}
}

func convertSubscribe(obj interface{}) (*v1.Subscription, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("cast type %T conversion to unstructured.Unstructured failed", obj)
	}
	set := &v1.Subscription{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.UnstructuredContent(), set)
	if err != nil {
		logrus.Errorf("convertStoreset Unstructured to Subscription error:%v", err)
		return nil, err
	}
	return set, nil
}
