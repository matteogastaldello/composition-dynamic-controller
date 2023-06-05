package helmchart

import (
	"context"
	"fmt"
	"strings"

	"github.com/gertd/go-pluralize"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/helmclient"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/text"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools"
	unstructuredtools "github.com/krateoplatformops/composition-dynamic-controller/internal/tools/unstructured"

	"helm.sh/helm/v3/pkg/action"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/scheme"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

func DeriveGroupVersionKind(cli helmclient.Client, url string) (schema.GroupVersionKind, error) {
	chart, _, err := cli.GetChart(url, &action.ChartPathOptions{})
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	name := chart.Metadata.Name
	version := chart.Metadata.Version

	pc := pluralize.NewClient()
	plural := strings.ToLower(pc.Plural(name))

	gvk := schema.GroupVersionKind{
		Group:   fmt.Sprintf("%s.krateo.io", plural),
		Version: fmt.Sprintf("v%s", strings.ReplaceAll(version, ".", "-")),
		Kind:    text.ToGolangName(name),
	}

	return gvk, nil
}

func ExtractValuesFromSpec(un *unstructured.Unstructured) ([]byte, error) {
	if un == nil {
		return nil, nil
	}

	spec, ok, err := unstructured.NestedMap(un.UnstructuredContent(), "spec")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	return yaml.Marshal(spec)
}

type RenderTemplateOptions struct {
	HelmClient helmclient.Client
	PackageURL string
	Resource   *unstructured.Unstructured
}

func RenderTemplate(ctx context.Context, opts RenderTemplateOptions) ([]unstructuredtools.ObjectRef, error) {
	dat, err := ExtractValuesFromSpec(opts.Resource)
	if err != nil {
		return nil, err
	}

	chartSpec := helmclient.ChartSpec{
		ReleaseName: opts.Resource.GetName(),
		Namespace:   opts.Resource.GetNamespace(),
		ChartName:   opts.PackageURL,
		ValuesYaml:  string(dat),
	}

	tpl, err := opts.HelmClient.TemplateChart(&chartSpec, nil)
	if err != nil {
		return nil, err
	}

	all := []unstructuredtools.ObjectRef{}

	decode := scheme.Codecs.UniversalDeserializer().Decode
	for _, spec := range strings.Split(string(tpl), "---") {
		if len(spec) == 0 {
			continue
		}
		obj, gvk, err := decode([]byte(spec), nil, nil)
		if err != nil {
			return all, err
		}

		el, ok := obj.(object)
		if !ok {
			continue
		}

		apiVersion, kind := gvk.ToAPIVersionAndKind()
		all = append(all, unstructuredtools.ObjectRef{
			APIVersion: apiVersion,
			Kind:       kind,
			Name:       el.GetName(),
			Namespace:  el.GetNamespace(),
		})
	}

	return all, nil
}

type CheckObjectsOptions struct {
	DynamicClient   dynamic.Interface
	DiscoveryClient *discovery.DiscoveryClient
}

func CheckObjects(ctx context.Context, objects []unstructuredtools.ObjectRef, opts CheckObjectsOptions) (*unstructuredtools.ObjectRef, error) {
	for _, el := range objects {
		gvr, err := tools.GVKtoGVR(opts.DiscoveryClient, schema.FromAPIVersionAndKind(el.APIVersion, el.Kind))
		if err != nil {
			return nil, err
		}

		un, err := opts.DynamicClient.Resource(gvr).
			Namespace(el.Namespace).
			Get(ctx, el.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		ok, err := unstructuredtools.IsAvailable(un)
		if err != nil {
			if ex, ok := err.(*unstructuredtools.NotAvailableError); ok {
				return ex.FailedObjectRef, nil
			}
			return nil, err
		}
		if ok {
			continue
		}

		return &unstructuredtools.ObjectRef{
			APIVersion: el.APIVersion,
			Kind:       el.Kind,
			Name:       el.Name,
			Namespace:  el.Namespace,
		}, nil
	}

	return nil, nil
}

type object interface {
	metav1.Object
	runtime.Object
}
