package storeset

import (
	"context"
	"encoding/json"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
)

var watchers = make([]func(ctx context.Context, client dynamic.Interface) error, 0)

func StartListWatcher(ctx context.Context) error {
	var config *rest.Config
	home := homedir.HomeDir()
	join := filepath.Join(home, ".kube", "config")
	_, err := os.Stat(join)
	if err == nil {
		config, err = clientcmd.BuildConfigFromFlags("", join)
		if err != nil {
			return err
		}
	} else {
		if os.IsNotExist(err) {
			config, err = rest.InClusterConfig()
			if err != nil {
				return err
			}
		}
	}
	if config == nil {
		return fmt.Errorf("kubeconfig not found")
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	for _, watcher := range watchers {
		err = watcher(ctx, dynamicClient)
		if err != nil {
			return err
		}
	}

	return nil
}

func convertLabelSelector() (labels.Selector, error) {
	if len(selector) == 0 {
		return labels.Everything(), nil
	}
	metaSelector := &metav1.LabelSelector{}
	if err := json.Unmarshal([]byte(selector), metaSelector); err != nil {
		return nil, err
	}
	newSelector := labels.NewSelector()
	for k, v := range metaSelector.MatchLabels {
		requirement, err := labels.NewRequirement(k, selection.Equals, []string{v})
		if err != nil {
			return nil, err
		}
		newSelector.Add(*requirement)
	}
	for _, expression := range metaSelector.MatchExpressions {
		var op selection.Operator
		switch expression.Operator {
		case metav1.LabelSelectorOpIn:
			op = selection.In
		case metav1.LabelSelectorOpNotIn:
			op = selection.NotIn
		case metav1.LabelSelectorOpExists:
			op = selection.Exists
		case metav1.LabelSelectorOpDoesNotExist:
			op = selection.DoesNotExist
		}

		requirement, err := labels.NewRequirement(expression.Key, op, expression.Values)
		if err != nil {
			return nil, err
		}
		newSelector.Add(*requirement)
	}
	return newSelector, nil
}
