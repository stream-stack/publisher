package storeset

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	v12 "github.com/stream-stack/common/crd/storeset/v1"
	"github.com/stream-stack/common/protocol/operator"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"time"
)

func init() {
	watchers = append(watchers, watchStoreset)
}

func watchStoreset(ctx context.Context, client dynamic.Interface) error {
	newSelector, err := convertLabelSelector()
	if err != nil {
		return err
	}

	listOps := metav1.ListOptions{
		LabelSelector: newSelector.String(),
		//ISSUE: https://github.com/kubernetes/kubernetes/issues/51046
		//FieldSelector: fields.OneTermEqualSelector("status.ready", "true").String(),
	}
	gvr := schema.GroupVersionResource{Group: "core.stream-stack.tanx", Version: "v1", Resource: "storesets"}
	informerFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, time.Hour, "", func(options *metav1.ListOptions) {
		options.LabelSelector = listOps.LabelSelector
	})
	resource := informerFactory.ForResource(gvr)
	handler := &storesetResourceEventHandler{ctx: ctx, client: client}
	resource.Informer().AddEventHandler(handler)

	logrus.Debugf("start informer at namespace [%s] for %s/%s/%s", "", gvr.Group, gvr.Version, gvr.Resource)
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())
	return nil
}

type storesetResourceEventHandler struct {
	ctx    context.Context
	client dynamic.Interface
}

func (s *storesetResourceEventHandler) OnAdd(obj interface{}) {
	store, err := convert(obj)
	if err != nil {
		logrus.Errorf("convertStoreset k8s storeset(crd) to storeset error:%v", err)
		return
	}
	if store == nil {
		return
	}
	logrus.Debugf("received storeset %s/%s add", store.Namespace, store.Name)
	StoreSetOpCh <- func(ctx context.Context, m map[string]*StoreSetConn) {
		getOrCreateConn(ctx, m, store, s.client)
	}
}

func (s *storesetResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	//??????????????????,sts???svc url??????????????????
	logrus.Debugf("unsupport storeset update,ignore")
}

func (s *storesetResourceEventHandler) OnDelete(obj interface{}) {
	store, err := convert(obj)
	if err != nil {
		logrus.Errorf("convertStoreset k8s storeset(crd) to storeset error:%v", err)
		return
	}
	if store == nil {
		return
	}
	logrus.Debugf("received storeset %s/%s delete", store.Namespace, store.Name)
	StoreSetOpCh <- func(ctx context.Context, m map[string]*StoreSetConn) {
		conn := getOrCreateConn(ctx, m, store, s.client)
		conn.Stop()
	}
}

func convert(obj interface{}) (*operator.StoreSet, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("cast type %T conversion to unstructured.Unstructured failed", obj)
	}
	set := &v12.StoreSet{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.UnstructuredContent(), set)
	if err != nil {
		logrus.Errorf("convertStoreset Unstructured to storeset error:%v", err)
		return nil, err
	}
	if set.Status.Status != v12.StoreSetStatusReady {
		logrus.Debugf("storeset %s/%s status %s not ready,ignore", set.Namespace, set.Name, set.Status.Status)
		return nil, nil
	}
	store := NewStoreset(set)
	return store, nil
}

func NewStoreset(set *v12.StoreSet) *operator.StoreSet {
	s := &operator.StoreSet{
		Name:      set.Name,
		Namespace: set.Namespace,
		Uris:      buildStoreUri(set),
	}
	return s
}

func buildStoreUri(item *v12.StoreSet) []string {
	replicas := *item.Spec.Store.Replicas
	addrs := make([]string, replicas)
	var i int32
	for ; i < replicas; i++ {
		//TODO:?????????????????????,?????????????????????,??????template??????????????????
		addrs[i] = fmt.Sprintf(`%s-%d.%s.%s:%s`, item.Name, i, item.Status.StoreStatus.ServiceName, item.Namespace, item.Spec.Store.Port)
	}
	return addrs
}
