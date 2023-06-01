package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/runtime"
)

const (
	maxRetries = 3
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

	c.logger.Error().Msgf("error processing event: %v, max retries reached", err)
	c.queue.Forget(obj)
	runtime.HandleError(err)
}

func (c *Controller) processItem(ctx context.Context, obj interface{}) error {
	evt, ok := obj.(event)
	if !ok {
		c.logger.Error().Msgf("unexpected event: %v", obj)
		return nil
	}
	switch evt.eventType {
	case objectAdd:
		return c.handleAddEvent(ctx, evt.objKey)
	case objectUpdate:
		return c.handleUpdateEvent(ctx, evt.objKey)
	case objectDelete:
		return c.handleDeleteEvent(ctx, evt.objKey)
	default:
		return nil
	}
}

func (c *Controller) handleAddEvent(ctx context.Context, key string) error {
	handler := c.funcs[objectAdd]
	if handler == nil {
		c.logger.Warn().
			Str("eventType", string(objectAdd)).
			Str("key", key).
			Msg("No event handler registered.")
		return nil
	}

	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		c.logger.Error().Str("key", key).Err(err).Msg("Fetching object.")
		return err
	}

	if !exists {
		c.logger.Warn().Str("key", key).Msg("Object does not exists anymore.")
		return nil
	}

	el := obj.(*unstructured.Unstructured)
	return handler(ctx, *el.DeepCopy())
}

func (c *Controller) handleUpdateEvent(ctx context.Context, key string) error {
	handler := c.funcs[objectAdd]
	if handler == nil {
		c.logger.Warn().
			Str("eventType", string(objectUpdate)).
			Str("key", key).
			Msg("No event handler registered.")
		return nil
	}

	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		c.logger.Error().Str("key", key).Err(err).Msg("Fetching object.")
		return err
	}

	if !exists {
		c.logger.Warn().Str("key", key).Msg("Object does not exists anymore.")
		return nil
	}

	el := obj.(*unstructured.Unstructured)
	return handler(ctx, *el.DeepCopy())
}

func (c *Controller) handleDeleteEvent(ctx context.Context, key string) error {
	c.logger.Debug().Str("key", key).Msg("Deleted.")
	return nil // NOOP
}
