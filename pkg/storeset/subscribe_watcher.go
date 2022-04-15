package storeset

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	v12 "github.com/stream-stack/publisher/pkg/crd/knative/v1"
	"github.com/stream-stack/publisher/pkg/proto"
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
	subscribe, err := convertSubscribe(obj)
	if err != nil {
		logrus.Errorf("convertSubscribe k8s subscribe(crd) to subscribe error:%v", err)
		return
	}
	if subscribe == nil {
		return
	}
	logrus.Debugf("received subscribe %s add", subscribe.Name)
	StoreSetOpCh <- func(ctx context.Context, conns map[string]*StoreSetConn) {
		subscribes[subscribe.Name] = subscribe
		for _, conn := range conns {
			conn.RunnerOpCh <- func(ctx context.Context, runners map[string]*SubscriberRunner) {
				runner, ok := runners[subscribe.Name]
				if !ok {
					runner = NewSubscribeRunner(subscribe, conn.conn, s.client)
					runners[subscribe.Name] = runner
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
	subscribe, err := convertSubscribe(obj)
	if err != nil {
		logrus.Errorf("convertSubscribe k8s subscribe(crd) to subscribe error:%v", err)
		return
	}
	if subscribe == nil {
		return
	}
	logrus.Debugf("received subscribe %s delete", subscribe.Name)
	StoreSetOpCh <- func(ctx context.Context, conns map[string]*StoreSetConn) {
		subscribes[subscribe.Name] = subscribe
		for _, conn := range conns {
			conn.RunnerOpCh <- func(ctx context.Context, runners map[string]*SubscriberRunner) {
				runner, ok := runners[subscribe.Name]
				if ok {
					runner.Stop()
				}
			}
		}
	}
}

func convertSubscribe(obj interface{}) (*proto.Subscribe, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("cast type %T conversion to unstructured.Unstructured failed", obj)
	}
	set := &v12.Subscription{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.UnstructuredContent(), set)
	if err != nil {
		logrus.Errorf("convertStoreset Unstructured to Subscription error:%v", err)
		return nil, err
	}

	return &proto.Subscribe{
		Name: set.Name,
		//TODO:生成uri
		Uri: "http://www.baidu.com",
	}, nil
}
