package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/helmclient"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools/helmchart"

	unstructuredtools "github.com/krateoplatformops/composition-dynamic-controller/internal/tools/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rs/zerolog"
	"helm.sh/helm/v3/pkg/release"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// externalObservation is the result of an observation of an external resource.
type externalObservation struct {
	// ResourceExists must be true if a corresponding external resource exists
	// for the managed resource.
	resourceExists bool

	// ResourceUpToDate should be true if the corresponding external resource
	// appears to be up-to-date - i.e. updating the external resource to match
	// the desired state of the managed resource would be a no-op.
	resourceUpToDate bool
}

type HelmChartPackage struct {
	// URL of the helm chart package that is being requested.
	URL string `json:"url"`

	// Version of the chart release.
	// +optional
	Version *string `json:"version,omitempty"`
}

type PackageInfoGetter interface {
	GetPackage(ctx context.Context) (*HelmChartPackage, error)
}

type observer struct {
	helmClient        helmclient.Client
	dynamicClient     *dynamic.DynamicClient
	discoveryClient   *discovery.DiscoveryClient
	logger            *zerolog.Logger
	packageInfoGetter PackageInfoGetter
}

func (o *observer) observe(ctx context.Context, cr *unstructured.Unstructured) (externalObservation, error) {
	if o.packageInfoGetter == nil {
		return externalObservation{}, fmt.Errorf("helm chart package info getter must be specified")
	}

	rel, err := o.findRelease(cr.GetName())
	if err != nil {
		if !errors.Is(err, errReleaseNotFound) {
			return externalObservation{}, err
		}
	}

	if rel == nil {
		return externalObservation{
			resourceExists:   false,
			resourceUpToDate: true,
		}, nil
	}

	pkg, err := o.packageInfoGetter.GetPackage(ctx)
	if err != nil {
		return externalObservation{}, err
	}

	all, err := helmchart.RenderTemplate(ctx, helmchart.RenderTemplateOptions{
		HelmClient: o.helmClient,
		Resource:   cr,
		PackageURL: pkg.URL,
	})
	if err != nil {
		return externalObservation{}, err
	}
	if len(all) == 0 {
		//if err := unstructuredtools.SetCondition(cr, unstructuredtools.Available()); err != nil {
		//	return  externalObservation{}, err
		//}

		return externalObservation{
			resourceExists:   true,
			resourceUpToDate: true,
		}, nil
	}

	ref, err := helmchart.CheckObjects(ctx, all, helmchart.CheckObjectsOptions{
		DynamicClient:   o.dynamicClient,
		DiscoveryClient: o.discoveryClient,
	})
	if err != nil {
		return externalObservation{}, err
	}
	if ref == nil {
		return externalObservation{
			resourceExists:   true,
			resourceUpToDate: true,
		}, nil
	}

	if err := unstructuredtools.SetFailedObjectRef(cr, ref); err != nil {
		return externalObservation{}, err
	}

	err = unstructuredtools.SetCondition(cr, metav1.Condition{
		Type:               unstructuredtools.TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             unstructuredtools.ReasonUnavailable,
	})
	if err != nil {
		return externalObservation{}, err
	}

	return externalObservation{
		resourceExists:   true,
		resourceUpToDate: true,
	}, nil
}

var (
	errReleaseNotFound = errors.New("helm release not found")
)

func (o *observer) findRelease(name string) (*release.Release, error) {
	all, err := o.helmClient.ListDeployedReleases()
	if err != nil {
		return nil, err
	}

	var res *release.Release
	for _, el := range all {
		if name == el.Name {
			res = el
			break
		}
	}

	return res, nil
}
