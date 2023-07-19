package archive

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
)

func TestDynamic(t *testing.T) {
	cfg, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	if err != nil {
		t.Fatal(err)
	}

	getter, err := Dynamic(cfg)
	if err != nil {
		t.Fatal(err)
	}

	gvk := schema.GroupVersionKind{
		Group:   "dummy-charts.krateo.io",
		Version: "v0-2-0",
		Kind:    "DummyChart",
	}

	info, err := getter.Get(gvk, "krateo-system")
	if err != nil {
		t.Fatal(err)
	}

	if len(info.URL) == 0 {
		t.Fatalf("expected valid log url; got empty")
	}

	t.Logf(info.URL)
}
