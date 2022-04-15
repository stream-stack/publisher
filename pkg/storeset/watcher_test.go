package storeset

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "github.com/stream-stack/publisher/pkg/crd/storeset/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartListWatcher(t *testing.T) {
	var config *rest.Config
	home := homedir.HomeDir()
	join := filepath.Join(home, ".kube", "config")
	join = "D://c1-config"
	_, err := os.Stat(join)
	if err == nil {
		config, err = clientcmd.BuildConfigFromFlags("", "D://c1-config")
		//config, err = clientcmd.BuildConfigFromFlags("", join)
		if err != nil {
			panic(err)
		}
	} else {
		if os.IsNotExist(err) {
			config, err = rest.InClusterConfig()
			if err != nil {
				panic(err)
			}
		}
	}
	if config == nil {
		panic(fmt.Errorf("kubeconfig not found"))
	}

	labelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			"test": `test`,
		},
	}
	marshal, _ := json.Marshal(labelSelector)

	selector = string(marshal)

	newSelector, err := convertLabelSelector()
	if err != nil {
		panic(err)
	}
	fmt.Println(newSelector)

	listOps := metav1.ListOptions{
		LabelSelector: newSelector.String(),
		//ISSUE: https://github.com/kubernetes/kubernetes/issues/51046
		//FieldSelector: fields.OneTermEqualSelector("status.status", "ready").String(),
	}
	fmt.Println(listOps)

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	gvr := schema.GroupVersionResource{Group: "core.stream-stack.tanx", Version: "v1", Resource: "storesets"}
	informerFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynamicClient, time.Hour, "default", func(options *metav1.ListOptions) {
		options.LabelSelector = listOps.LabelSelector
	})
	u, err := dynamicClient.Resource(gvr).List(context.TODO(), listOps)
	fmt.Println(u, err)

	//informerFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, time.Hour)
	resource := informerFactory.ForResource(gvr)
	handler := &storesetResourceEventHandler{}
	resource.Informer().AddEventHandler(handler)

	c := make(chan struct{})
	informerFactory.Start(c)
	informerFactory.WaitForCacheSync(c)
	list, err := resource.Lister().List(newSelector)
	//list, err := resource.Lister().List(labels.Everything())
	if err != nil {
		panic(err)
	}
	for _, object := range list {
		u, ok := object.(*unstructured.Unstructured)

		fmt.Println(u, ok)
		set := &v1.StoreSet{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.UnstructuredContent(), set)
		if err != nil {
			panic(err)
		}
	}
}
