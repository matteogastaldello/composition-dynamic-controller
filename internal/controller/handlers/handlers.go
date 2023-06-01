package handlers

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type createFunc func(context.Context, unstructured.Unstructured) error
type updateFunc func(context.Context, unstructured.Unstructured) error
