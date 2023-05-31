package listwatcher

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

type CreateOptions struct {
	Client    dynamic.Interface
	GVR       schema.GroupVersionResource
	Namespace string
}

func Create(opts CreateOptions) *cache.ListWatch {
	return &cache.ListWatch{
		ListFunc: func(lo metav1.ListOptions) (runtime.Object, error) {
			return opts.Client.Resource(opts.GVR).
				Namespace(opts.Namespace).
				List(context.Background(), lo)
		},
		WatchFunc: func(lo metav1.ListOptions) (watch.Interface, error) {
			return opts.Client.Resource(opts.GVR).
				Namespace(opts.Namespace).
				Watch(context.Background(), lo)
		},
	}
}
