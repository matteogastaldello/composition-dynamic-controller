package handlers

import (
	"github.com/krateoplatformops/composition-dynamic-controller/internal/helmclient"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/meta"

	"github.com/rs/zerolog"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func Connect(logger *zerolog.Logger, cr *unstructured.Unstructured) (helmclient.Client, error) {
	opts := &helmclient.Options{
		Namespace:        cr.GetNamespace(),
		RepositoryCache:  "/tmp/.helmcache",
		RepositoryConfig: "/tmp/.helmrepo",
		Debug:            true,
		Linting:          false,
		DebugLog: func(format string, v ...interface{}) {
			if !meta.IsVerbose(cr) {
				return
			}
			if len(v) > 0 {
				logger.Debug().Msgf(format, v)
			} else {
				logger.Debug().Msg(format)
			}
		},
	}

	return helmclient.New(opts)
}
