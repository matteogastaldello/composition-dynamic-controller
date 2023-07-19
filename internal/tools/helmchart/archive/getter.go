package archive

import (
	"context"
	"fmt"

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
	Get(gvk schema.GroupVersionKind, namespace string) (Info, error)
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

func (pig staticGetter) Get(_ schema.GroupVersionKind, _ string) (Info, error) {
	return Info{
		URL: pig.chartName,
	}, nil
}

const (
	keyCrdGroup   = "krateo.io/crd-group"
	keyCrdVersion = "krateo.io/crd-version"
	keyCrdKind    = "krateo.io/crd-kind"
)

var _ Getter = (*dynamicGetter)(nil)

type dynamicGetter struct {
	dynamicClient dynamic.Interface
}

func (g *dynamicGetter) Get(gvk schema.GroupVersionKind, namespace string) (Info, error) {
	gvr := schema.GroupVersionResource{
		Group:    "krateo.io",
		Version:  "v1alpha1",
		Resource: "definitions",
	}

	sel, err := g.selectorForGVK(gvk)
	if err != nil {
		return Info{}, err
	}

	all, err := g.dynamicClient.Resource(gvr).Namespace(namespace).
		List(context.Background(), metav1.ListOptions{
			LabelSelector: sel,
		})
	if err != nil {
		return Info{}, err
	}

	switch tot := len(all.Items); {
	case tot == 0:
		return Info{},
			fmt.Errorf("no definition found for %v in namespace: %s", gvk, namespace)
	case tot == 1:
		url, ok, err := unstructured.NestedString(all.Items[0].Object, "spec", "chartUrl")
		if err != nil {
			return Info{}, err
		}
		if !ok {
			return Info{},
				fmt.Errorf("missing spec.chartUrl value in definition for %v in namespace: %s", gvk, namespace)
		}
		return Info{URL: url}, nil
	default:
		return Info{},
			fmt.Errorf("found %d definitions for %v in namespace: %s", tot, gvk, namespace)
	}
}

func (g *dynamicGetter) selectorForGVK(gvk schema.GroupVersionKind) (string, error) {
	group, err := labels.NewRequirement(keyCrdGroup, selection.Equals, []string{gvk.Group})
	if err != nil {
		return "", err
	}

	version, err := labels.NewRequirement(keyCrdVersion, selection.Equals, []string{gvk.Version})
	if err != nil {
		return "", err
	}

	kind, err := labels.NewRequirement(keyCrdKind, selection.Equals, []string{gvk.Kind})
	if err != nil {
		return "", err
	}

	selector := labels.NewSelector().Add(*group, *version, *kind)

	return selector.String(), nil
}
