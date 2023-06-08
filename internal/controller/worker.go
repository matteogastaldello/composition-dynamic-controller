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
	switch evt.eventType {
	case Observe:
		return c.handleObserve(ctx, evt.objKey)
	case Create:
		return c.handleCreate(ctx, evt.objKey)
	case Update:
		return c.handleUpdateEvent(ctx, evt.objKey)
	case Delete:
		return c.handleDeleteEvent(ctx, evt.objKey)
	default:
		return nil
	}
}

func (c *Controller) handleObserve(ctx context.Context, key string) error {
	if c.externalClient == nil {
		c.logger.Warn().
			Str("eventType", string(Observe)).
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
	exists, err = c.externalClient.Observe(ctx, el.DeepCopy())
	if err != nil {
		return err
	}

	if !exists {
		c.queue.Add(event{
			eventType: Create,
			objKey:    key,
		})
	}

	return nil
}

func (c *Controller) handleCreate(ctx context.Context, key string) error {
	if c.externalClient == nil {
		c.logger.Warn().
			Str("eventType", string(Create)).
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
	return c.externalClient.Create(ctx, el.DeepCopy())
}

func (c *Controller) handleUpdateEvent(ctx context.Context, key string) error {
	if c.externalClient == nil {
		c.logger.Warn().
			Str("eventType", string(Update)).
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
	return c.externalClient.Update(ctx, el.DeepCopy())
}

func (c *Controller) handleDeleteEvent(ctx context.Context, key string) error {
	if c.externalClient == nil {
		c.logger.Warn().
			Str("eventType", string(Delete)).
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
	return c.externalClient.Delete(ctx, el.DeepCopy())
}
