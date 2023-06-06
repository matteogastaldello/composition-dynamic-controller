package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/listwatcher"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

type Options struct {
	Client          dynamic.Interface
	DiscoveryClient *discovery.DiscoveryClient
	GVR             schema.GroupVersionResource
	Namespace       string
	ResyncInterval  time.Duration
	Recorder        record.EventRecorder
	Logger          *zerolog.Logger
	ExternalClient  ExternalClient
}

type Controller struct {
	dynamicClient   dynamic.Interface
	discoveryClient *discovery.DiscoveryClient
	queue           workqueue.RateLimitingInterface
	indexer         cache.Indexer
	informer        cache.Controller
	recorder        record.EventRecorder
	logger          *zerolog.Logger
	externalClient  ExternalClient
}

// New creates a new Controller.
func New(opts Options) *Controller {
	rateLimiter := workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(3*time.Second, 180*time.Second),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)

	queue := workqueue.NewRateLimitingQueue(rateLimiter)

	indexer, informer := cache.NewIndexerInformer(
		listwatcher.Create(listwatcher.CreateOptions{
			Client:    opts.Client,
			GVR:       opts.GVR,
			Namespace: opts.Namespace,
		}),
		&unstructured.Unstructured{},
		opts.ResyncInterval,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				key, err := cache.MetaNamespaceKeyFunc(obj)
				if err != nil {
					opts.Logger.Warn().Err(err).Msg("Getting meta namespace key.")
					return
				}

				queue.Add(event{
					eventType: Observe,
					objKey:    key,
				})
			},
			UpdateFunc: func(old, new interface{}) {
				key, err := cache.MetaNamespaceKeyFunc(new)
				if err != nil {
					opts.Logger.Warn().Err(err).Msg("Getting meta namespace key.")
					return
				}

				oldUns, ok := old.(*unstructured.Unstructured)
				if !ok {
					opts.Logger.Warn().Msg("UpdateFunc: object is not an unstructured.")
					return
				}

				newUns, ok := new.(*unstructured.Unstructured)
				if !ok {
					opts.Logger.Warn().Msg("UpdateFunc: object is not an unstructured.")
					return
				}

				if oldUns.GetGeneration() == newUns.GetGeneration() {
					queue.Add(event{
						eventType: Observe,
						objKey:    key,
					})
					return
				}

				queue.Add(event{
					eventType: Update,
					objKey:    key,
				})
			},
			DeleteFunc: func(obj interface{}) {
				key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
				if err == nil {
					queue.Add(event{
						eventType: Delete,
						objKey:    key,
					})
				}
			},
		},
		cache.Indexers{},
	)

	return &Controller{
		dynamicClient:   opts.Client,
		discoveryClient: opts.DiscoveryClient,
		recorder:        opts.Recorder,
		logger:          opts.Logger,
		informer:        informer,
		indexer:         indexer,
		queue:           queue,
		externalClient:  opts.ExternalClient,
	}
}

func (c *Controller) SetExternalClient(ec ExternalClient) {
	c.externalClient = ec
}

// Run begins watching and syncing.
func (c *Controller) Run(ctx context.Context, numWorkers int) error {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info().Msg("Starting controller")
	go c.informer.Run(ctx.Done())

	// Wait for all involved caches to be synced, before
	// processing items from the queue is started
	c.logger.Info().Msg("waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), c.informer.HasSynced) {
		err := fmt.Errorf("failed to wait for informers caches to sync")
		utilruntime.HandleError(err)
		return err
	}

	c.logger.Info().Int("workers", numWorkers).Msg("Starting workers.")
	for i := 0; i < numWorkers; i++ {
		go wait.Until(func() {
			c.runWorker(ctx)
		}, 2*time.Second, ctx.Done())
	}
	c.logger.Info().Msg("Controller ready.")

	<-ctx.Done()
	c.logger.Info().Msg("Stopping controller.")

	return nil
}
