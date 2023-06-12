package controller

import (
	"context"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
)

const (
	maxRetries = 5
)

func (c *Controller) runWorker(ctx context.Context) {
	for {
		obj, shutdown := c.queue.Get()
		if shutdown {
			break
		}
		defer c.queue.Done(obj)

		err := c.processItem(ctx, obj)
		c.handleErr(err, obj)
	}
}

func (c *Controller) handleErr(err error, obj interface{}) {
	if err == nil {
		c.queue.Forget(obj)
		return
	}

	if c.queue.NumRequeues(obj) < maxRetries {
		c.logger.Warn().Msgf("error processing event: %v, retrying", err)
		c.queue.AddRateLimited(obj)
		return
	}

	c.logger.Error().Err(err).Msg("error processing event (max retries reached)")
	c.queue.Forget(obj)
	runtime.HandleError(err)
}

func (c *Controller) processItem(ctx context.Context, obj interface{}) error {
	evt, ok := obj.(event)
	if !ok {
		c.logger.Error().Msgf("unexpected event: %v", obj)
		return nil
	}

	c.logger.Debug().Str("event", string(evt.eventType)).Str("ref", evt.objectRef.String()).Msg("processing")
	switch evt.eventType {
	case Create:
		return c.handleCreate(ctx, evt.objectRef)
	case Update:
		return c.handleUpdateEvent(ctx, evt.objectRef)
	case Delete:
		return c.handleDeleteEvent(ctx, evt.objectRef)
	default:
		return c.handleObserve(ctx, evt.objectRef)
	}
}

func (c *Controller) handleObserve(ctx context.Context, ref ObjectRef) error {
	if c.externalClient == nil {
		c.logger.Warn().
			Str("eventType", string(Observe)).
			Msg("No event handler registered.")
		return nil
	}

	el, err := c.fetch(ctx, ref)
	if err != nil {
		c.logger.Err(err).
			Str("objectRef", ref.String()).
			Msg("Resolving unstructured object.")
		return err
	}

	_, err = c.externalClient.Observe(ctx, el.DeepCopy())
	if err != nil {
		return err
	}

	/*
		if !exists {
			c.queue.Add(event{
				eventType: Create,
				objectRef: ref,
			})
		}*/

	return nil
}

func (c *Controller) handleCreate(ctx context.Context, ref ObjectRef) error {
	if c.externalClient == nil {
		c.logger.Warn().
			Str("eventType", string(Create)).
			Msg("No event handler registered.")
		return nil
	}

	el, err := c.fetch(ctx, ref)
	if err != nil {
		c.logger.Err(err).
			Str("objectRef", ref.String()).
			Msg("Resolving unstructured object.")
		return err
	}

	return c.externalClient.Create(ctx, el.DeepCopy())
}

func (c *Controller) handleUpdateEvent(ctx context.Context, ref ObjectRef) error {
	if c.externalClient == nil {
		c.logger.Warn().
			Str("eventType", string(Update)).
			Msg("No event handler registered.")
		return nil
	}

	el, err := c.fetch(ctx, ref)
	if err != nil {
		c.logger.Err(err).
			Str("objectRef", ref.String()).
			Msg("Resolving unstructured object.")
		return err
	}

	return c.externalClient.Update(ctx, el.DeepCopy())
}

func (c *Controller) handleDeleteEvent(ctx context.Context, ref ObjectRef) error {
	if c.externalClient == nil {
		c.logger.Warn().
			Str("eventType", string(Delete)).
			Msg("No event handler registered.")
		return nil
	}

	return c.externalClient.Delete(ctx, ref)
}

func (c *Controller) fetch(ctx context.Context, ref ObjectRef) (*unstructured.Unstructured, error) {
	gvr, err := tools.GVKtoGVR(c.discoveryClient, schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind))
	if err != nil {
		return nil, err
	}

	return c.dynamicClient.Resource(gvr).
		Namespace(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
}
