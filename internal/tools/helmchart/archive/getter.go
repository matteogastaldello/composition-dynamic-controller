package archive

import (
	"context"
	"fmt"

	unstructuredtools "github.com/krateoplatformops/composition-dynamic-controller/internal/tools/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type Info struct {
	// URL of the helm chart package that is being requested.
	URL string `json:"url"`

	// Version of the chart release.
	// +optional
	Version *string `json:"version,omitempty"`
}

type Getter interface {
	Get(un *unstructured.Unstructured) (Info, error)
}

func Static(chart string) Getter {
	return staticGetter{chartName: chart}
}

func Dynamic(cfg *rest.Config) (Getter, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &dynamicGetter{
		dynamicClient: dyn,
	}, nil
}

var _ Getter = (*staticGetter)(nil)

type staticGetter struct {
	chartName string
}

func (pig staticGetter) Get(_ *unstructured.Unstructured) (Info, error) {
	return Info{
		URL: pig.chartName,
	}, nil
}

const (
	labelKeyGroup    = "krateo.io/crd-group"
	labelKeyVersion  = "krateo.io/crd-version"
	labelKeyResource = "krateo.io/crd-resource"
)

var _ Getter = (*dynamicGetter)(nil)

type dynamicGetter struct {
	dynamicClient dynamic.Interface
}

func (g *dynamicGetter) Get(un *unstructured.Unstructured) (Info, error) {
	gvr, err := unstructuredtools.GVR(un)
	if err != nil {
		return Info{}, err
	}

	sel, err := g.selectorForGVR(gvr)
	if err != nil {
		return Info{}, err
	}

	gvrForDefinitions := schema.GroupVersionResource{
		Group:    "core.krateo.io",
		Version:  "v1alpha1",
		Resource: "definitions",
	}

	all, err := g.dynamicClient.Resource(gvrForDefinitions).
		Namespace(un.GetNamespace()).
		List(context.Background(), metav1.ListOptions{
			LabelSelector: sel,
		})
	if err != nil {
		return Info{}, err
	}

	switch tot := len(all.Items); {
	case tot == 0:
		return Info{},
			fmt.Errorf("no definition found for '%v' in namespace: %s", gvr, un.GetNamespace())
	case tot == 1:
		url, ok, err := unstructured.NestedString(all.Items[0].Object, "spec", "chartUrl")
		if err != nil {
			return Info{}, err
		}
		if !ok {
			return Info{},
				fmt.Errorf("missing spec.chartUrl value in definition for '%v' in namespace: %s", gvr, un.GetNamespace())
		}
		return Info{URL: url}, nil
	default:
		return Info{},
			fmt.Errorf("found %d definitions for '%v' in namespace: %s", tot, gvr, un.GetNamespace())
	}
}

func (g *dynamicGetter) selectorForGVR(gvr schema.GroupVersionResource) (string, error) {
	group, err := labels.NewRequirement(labelKeyGroup, selection.Equals, []string{gvr.Group})
	if err != nil {
		return "", err
	}

	version, err := labels.NewRequirement(labelKeyVersion, selection.Equals, []string{gvr.Version})
	if err != nil {
		return "", err
	}

	resource, err := labels.NewRequirement(labelKeyResource, selection.Equals, []string{gvr.Resource})
	if err != nil {
		return "", err
	}

	selector := labels.NewSelector().Add(*group, *version, *resource)

	return selector.String(), nil
}
