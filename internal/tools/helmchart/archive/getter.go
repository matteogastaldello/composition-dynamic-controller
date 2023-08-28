package archive

import (
	"context"
	"fmt"
	"strings"

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
	Version string `json:"version,omitempty"`
}

func (i *Info) IsOCI() bool {
	return strings.HasPrefix(i.URL, "oci://")
}

func (i *Info) IsTGZ() bool {
	return strings.HasSuffix(i.URL, ".tgz")
}

func (i *Info) IsHTTP() bool {
	return strings.HasPrefix(i.URL, "http://") || strings.HasPrefix(i.URL, "https://")
}

type Getter interface {
	Get(un *unstructured.Unstructured) (*Info, error)
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

func (pig staticGetter) Get(_ *unstructured.Unstructured) (*Info, error) {
	return &Info{
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

func (g *dynamicGetter) Get(un *unstructured.Unstructured) (*Info, error) {
	gvr, err := unstructuredtools.GVR(un)
	if err != nil {
		return nil, err
	}

	sel, err := g.selectorForGVR(gvr)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	switch tot := len(all.Items); {
	case tot == 0:
		return nil,
			fmt.Errorf("no definition found for '%v' in namespace: %s", gvr, un.GetNamespace())
	case tot == 1:
		packageUrl, ok, err := unstructured.NestedString(all.Items[0].Object, "status", "packageUrl")
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil,
				fmt.Errorf("missing status.packageUrl in definition for '%v' in namespace: %s", gvr, un.GetNamespace())
		}

		packageVersion, _, err := unstructured.NestedString(all.Items[0].Object, "chart", "version")
		if err != nil {
			return nil, err
		}

		return &Info{URL: packageUrl, Version: packageVersion}, nil
	default:
		return nil,
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
