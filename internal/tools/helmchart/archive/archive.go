package archive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Info struct {
	// URL of the helm chart package that is being requested.
	URL string `json:"url"`

	// Version of the chart release.
	// +optional
	Version *string `json:"version,omitempty"`
}

type Getter interface {
	Get(gvk schema.GroupVersionKind, name, namespace string) (Info, error)
}

func Static(chart string) Getter {
	return staticGetter{chartName: chart}
}

func Remote(url string) Getter {
	return remoteGetter{url: url}
}

var _ Getter = (*staticGetter)(nil)

type staticGetter struct {
	chartName string
}

func (pig staticGetter) Get(_ schema.GroupVersionKind, _, _ string) (Info, error) {
	return Info{
		URL: pig.chartName,
	}, nil
}

var _ Getter = (*remoteGetter)(nil)

type remoteGetter struct {
	url string
}

func (pig remoteGetter) Get(gvk schema.GroupVersionKind, name, namespace string) (Info, error) {
	body := map[string]string{
		"group":     gvk.Group,
		"version":   gvk.Version,
		"kind":      gvk.Kind,
		"name":      name,
		"namespace": namespace,
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	if err := encoder.Encode(body); err != nil {
		return Info{}, err
	}

	req, err := http.NewRequest(http.MethodPost, pig.url, &buf)
	if err != nil {
		return Info{}, err
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return Info{}, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return Info{}, fmt.Errorf(
			"helm chart archive url not found (gvk: %v, name: %s, namespace: %s)", gvk, name, namespace)
	}

	nfo := Info{}
	err = json.NewDecoder(res.Body).Decode(&nfo)
	return nfo, err
}
