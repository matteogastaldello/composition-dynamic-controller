package handlers

import (
	"context"

	"github.com/rs/zerolog"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func HandleUpdate(logger *zerolog.Logger) createFunc {
	return func(ctx context.Context, el unstructured.Unstructured) error {
		logger.Debug().
			Str("apiVersion", el.GetAPIVersion()).
			Str("kind", el.GetKind()).
			Str("name", el.GetName()).
			Msg("Handling update event.")

		return nil
	}
}
