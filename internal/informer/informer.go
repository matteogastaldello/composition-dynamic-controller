package informer

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type Options struct {
	Client               dynamic.Interface
	ResyncInterval       time.Duration
	GroupVersionResource schema.GroupVersionResource
	Namespace            string
}

func Create(opts Options) (cache.SharedIndexInformer, error) {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		opts.Client, opts.ResyncInterval, opts.Namespace, nil)
	gi := factory.ForResource(opts.GroupVersionResource)
	if gi == nil {
		return nil, fmt.Errorf("unable to get generic informer for resource '%s'", opts.GroupVersionResource)
	}

	return gi.Informer(), nil
}

/*
// Run starts the Watcher.
func (w *ClaimInformer) Run(stopCh <-chan struct{}) {
	w.informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				w.onObject(obj)
			},
			UpdateFunc: func(_, newObj interface{}) {
				w.onObject(newObj)
			},
			DeleteFunc: func(obj interface{}) {
				w.onObject(obj)
			},
		},
	)

	defer utilruntime.HandleCrash()

	w.informer.Run(stopCh)
	//w.factory.Start(stopCh)

	// here is where we kick the caches into gear
	if !cache.WaitForCacheSync(stopCh, w.informer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	<-stopCh
}

func (w *ClaimInformer) onObject(obj interface{}) {
	unstr, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}

	status := w.findStatus(unstr)
	if status == nil || len(status.Conditions) == 0 {
		return
	}

	for _, cond := range status.Conditions {
		ref := corev1.ObjectReference{
			UID:             unstr.GetUID(),
			Kind:            unstr.GetKind(),
			Name:            unstr.GetName(),
			Namespace:       unstr.GetNamespace(),
			APIVersion:      unstr.GetAPIVersion(),
			ResourceVersion: unstr.GetResourceVersion(),
		}

		eventType := corev1.EventTypeNormal
		if cond.Status != ConditionStatus(corev1.ConditionTrue) {
			eventType = corev1.EventTypeWarning
		}

		w.recorder.Event(&ref, eventType, cond.Reason, cond.Message)
	}
}
*/
