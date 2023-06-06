package composition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/controller"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/helmclient"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/meta"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools/helmchart"

	"github.com/rs/zerolog"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools"

	unstructuredtools "github.com/krateoplatformops/composition-dynamic-controller/internal/tools/unstructured"
)

var (
	errReleaseNotFound  = errors.New("helm release not found")
	errCreateIncomplete = "cannot determine creation result - remove the " + meta.AnnotationKeyExternalCreatePending + " annotation if it is safe to proceed"
)

var _ controller.ExternalClient = (*handler)(nil)

func NewHandler(cfg *rest.Config, log *zerolog.Logger, pig helmchart.PackageInfoGetter) controller.ExternalClient {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Creating dynamic client.")
	}

	dis, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Creating discovery client.")
	}

	return &handler{
		logger:            log,
		dynamicClient:     dyn,
		discoveryClient:   dis,
		packageInfoGetter: pig,
	}
}

type handler struct {
	logger            *zerolog.Logger
	dynamicClient     dynamic.Interface
	discoveryClient   *discovery.DiscoveryClient
	packageInfoGetter helmchart.PackageInfoGetter
}

func (h *handler) Observe(ctx context.Context, mg *unstructured.Unstructured) (bool, error) {
	h.logger.Debug().Str("apiVersion", mg.GetAPIVersion()).
		Str("kind", mg.GetKind()).
		Str("name", mg.GetName()).
		Str("namespace", mg.GetNamespace()).
		Msg("Observing resource")

	if h.packageInfoGetter == nil {
		return false, fmt.Errorf("helm chart package info getter must be specified")
	}

	hc, err := h.helmClientForResource(mg)
	if err != nil {
		return false, err
	}

	rel, err := helmchart.FindRelease(hc, mg.GetName())
	if err != nil {
		if !errors.Is(err, errReleaseNotFound) {
			return false, err
		}
	}
	if rel == nil {
		h.logger.Debug().Str("release", mg.GetName()).Msg("Release not found.")
		return false, nil
	}

	pkg, err := h.packageInfoGetter.GetPackage(ctx)
	if err != nil {
		return false, err
	}
	h.logger.Debug().Str("release", mg.GetName()).Str("chart", pkg.URL).Msg("Package info fetched.")

	all, err := helmchart.RenderTemplate(ctx, helmchart.RenderTemplateOptions{
		HelmClient: hc,
		Resource:   mg,
		PackageURL: pkg.URL,
	})
	if err != nil {
		return false, err
	}
	if len(all) == 0 {
		return true, nil
	}

	ref, err := helmchart.CheckObjects(ctx, all, helmchart.CheckObjectsOptions{
		DynamicClient:   h.dynamicClient,
		DiscoveryClient: h.discoveryClient,
	})
	if err != nil {
		return false, err
	}

	if ref != nil {
		h.logger.Warn().Str("apiVersion", ref.APIVersion).
			Str("kind", ref.Kind).
			Str("name", ref.Name).
			Str("namespace", ref.Namespace).
			Msg("Founc composition failing object reference.")

		err := unstructuredtools.SetFailedObjectRef(mg, ref)
		if err != nil {
			return true, err
		}

		err = unstructuredtools.SetCondition(mg, unstructuredtools.Unavailable())
		if err != nil {
			return true, err
		}
	} else {
		err := unstructuredtools.SetCondition(mg, unstructuredtools.Available())
		if err != nil {
			return true, err
		}
	}

	return true, tools.UpdateStatus(ctx, mg, tools.UpdateStatusOptions{
		DiscoveryClient: h.discoveryClient,
		DynamicClient:   h.dynamicClient,
	})
}

func (h *handler) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	// If we started but never completed creation of an external resource we
	// may have lost critical information.The safest thing to
	// do is to refuse to proceed.
	if meta.ExternalCreateIncomplete(mg) {
		h.logger.Debug().Msg(errCreateIncomplete)
		err := unstructuredtools.SetCondition(mg, unstructuredtools.Creating())
		if err != nil {
			return err
		}
		return tools.UpdateStatus(ctx, mg, tools.UpdateStatusOptions{
			DiscoveryClient: h.discoveryClient,
			DynamicClient:   h.dynamicClient,
		})
	}

	meta.SetExternalCreatePending(mg, time.Now())
	if err := tools.UpdateStatus(ctx, mg, tools.UpdateStatusOptions{
		DiscoveryClient: h.discoveryClient,
		DynamicClient:   h.dynamicClient,
	}); err != nil {
		return err
	}

	if h.packageInfoGetter == nil {
		return fmt.Errorf("helm chart package info getter must be specified")
	}

	hc, err := h.helmClientForResource(mg)
	if err != nil {
		return err
	}

	pkg, err := h.packageInfoGetter.GetPackage(ctx)
	if err != nil {
		return err
	}

	_, gen, err := helmchart.Install(ctx, helmchart.InstallOptions{
		HelmClient: hc,
		ChartName:  pkg.URL,
		Resource:   mg,
	})
	if err != nil {
		meta.SetExternalCreateFailed(mg, time.Now())
		unstructuredtools.SetCondition(mg, unstructuredtools.CreatingFailed(
			fmt.Sprintf("Creating failed: %s", err.Error())))

		return tools.UpdateStatus(ctx, mg, tools.UpdateStatusOptions{
			DiscoveryClient: h.discoveryClient,
			DynamicClient:   h.dynamicClient,
		})
	}

	// TODO add gen to CRD generator
	h.logger.Debug().Msgf("generation: %d", gen)
	return nil
}

func (h *handler) Update(ctx context.Context, mg *unstructured.Unstructured) error {
	h.logger.Debug().Str("apiVersion", mg.GetAPIVersion()).
		Str("kind", mg.GetKind()).
		Str("name", mg.GetName()).
		Str("namespace", mg.GetNamespace()).
		Msg("Handling resource update.")

	return nil // NOOP
}

func (h *handler) Delete(ctx context.Context, mg *unstructured.Unstructured) error {
	return nil // NOOP
}

func (h *handler) helmClientForResource(mg *unstructured.Unstructured) (helmclient.Client, error) {
	opts := &helmclient.Options{
		Namespace:        mg.GetNamespace(),
		RepositoryCache:  "/tmp/.helmcache",
		RepositoryConfig: "/tmp/.helmrepo",
		Debug:            true,
		Linting:          false,
		DebugLog: func(format string, v ...interface{}) {
			if !meta.IsVerbose(mg) {
				return
			}
			if len(v) > 0 {
				h.logger.Debug().Msgf(format, v)
			} else {
				h.logger.Debug().Msg(format)
			}
		},
	}

	return helmclient.New(opts)
}
