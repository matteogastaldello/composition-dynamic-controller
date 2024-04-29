package composition

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/gobuffalo/flect"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/client/restclient"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/text"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools/apiaction"
	getter "github.com/krateoplatformops/composition-dynamic-controller/internal/tools/restclient"
	unstructuredtools "github.com/krateoplatformops/composition-dynamic-controller/internal/tools/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type RequestedParams struct {
	Parameters text.StringSet
	Query      text.StringSet
	Body       text.StringSet
}

type CallInfo struct {
	Path            string
	ReqParams       *RequestedParams
	IdentifierField string
	AltFields       map[string]string
}

type APIFuncDef func(ctx context.Context, cli *http.Client, path string, conf *restclient.RequestConfiguration) (*map[string]interface{}, error)

func APICallBuilder(cli *restclient.UnstructuredClient, info *getter.Info, action apiaction.APIAction) (apifunc APIFuncDef, callInfo *CallInfo, err error) {
	identifierField := info.Resource.Identifier
	for _, descr := range info.Resource.VerbsDescription {
		if strings.EqualFold(descr.Action, action.String()) {
			method, err := restclient.StringToApiCallType(descr.Method)
			if err != nil {
				return nil, nil, fmt.Errorf("error converting method to api call type: %s", err)
			}
			params, query, err := cli.RequestedParams(descr.Method, descr.Path)
			if err != nil {
				return nil, nil, fmt.Errorf("error retrieving requested params: %s", err)
			}
			var body text.StringSet
			if descr.Method == "POST" || descr.Method == "PUT" || descr.Method == "PATCH" {
				body, err = cli.RequestedBody(descr.Method, descr.Path)
				if err != nil {
					return nil, nil, fmt.Errorf("error retrieving requested body params: %s", err)
				}
			}

			callInfo := &CallInfo{
				Path: descr.Path,
				ReqParams: &RequestedParams{
					Parameters: params,
					Query:      query,
					Body:       body,
				},
				AltFields:       descr.AltFieldMapping,
				IdentifierField: identifierField,
			}
			switch method {
			case restclient.APICallsTypeGet:
				return cli.Get, callInfo, nil
			case restclient.APICallsTypePost:
				return cli.Post, callInfo, nil
			case restclient.APICallsTypeList:
				return cli.List, callInfo, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("impossible to build api call for action %s", action.String())
}

func BuildCallConfig(callInfo *CallInfo, statusFields map[string]interface{}, specFields map[string]interface{}) *restclient.RequestConfiguration {
	reqConfiguration := &restclient.RequestConfiguration{}
	reqConfiguration.Parameters = make(map[string]string)
	reqConfiguration.Query = make(map[string]string)
	mapBody := make(map[string]interface{})

	for field, value := range specFields {
		f, ok := callInfo.AltFields[field]
		if ok {
			field = f
		}
		if callInfo.ReqParams.Parameters.Contains(field) {
			stringVal := fmt.Sprintf("%v", value)

			reqConfiguration.Parameters[field] = stringVal
		} else if callInfo.ReqParams.Query.Contains(field) {
			stringVal := fmt.Sprintf("%v", value)
			reqConfiguration.Query[field] = stringVal
		} else if callInfo.ReqParams.Body.Contains(field) {
			mapBody[field] = value
		}
	}
	for field, value := range statusFields {
		f, ok := callInfo.AltFields[field]
		if ok {
			field = f
		}
		if callInfo.ReqParams.Parameters.Contains(field) {
			stringVal := fmt.Sprintf("%v", value)
			reqConfiguration.Parameters[field] = stringVal
		} else if callInfo.ReqParams.Query.Contains(field) {
			stringVal := fmt.Sprintf("%v", value)
			reqConfiguration.Query[field] = stringVal
		} else if callInfo.ReqParams.Body.Contains(field) {
			mapBody[field] = value
		}
	}
	reqConfiguration.Body = mapBody
	return reqConfiguration
}
func resolveObjectFromReferenceInfo(ref getter.ReferenceInfo, mg *unstructured.Unstructured, dyClient dynamic.Interface) (*unstructured.Unstructured, error) {
	gvrForReference := schema.GroupVersionResource{
		Group:    ref.GroupVersionKind.Group,
		Version:  ref.GroupVersionKind.Version,
		Resource: strings.ToLower(flect.Pluralize(ref.GroupVersionKind.Kind)),
	}

	all, err := dyClient.Resource(gvrForReference).
		List(context.Background(), metav1.ListOptions{
			// FieldSelector: fields.AndSelectors(fields.OneTermEqualSelector("krateo.io/crd-group", "group.group")).String(),
		})
	if err != nil {
		return nil, fmt.Errorf("error getting reference resource - %w", err)
	}
	if len(all.Items) == 0 {
		return nil, fmt.Errorf("no reference found for resource %s - len is zero", gvrForReference.Resource)
	}

	fieldValue, ok, err := unstructured.NestedString(mg.Object, "spec", ref.Field)
	if !ok {
		return nil, fmt.Errorf("spec field %s not found in reference resource", ref.Field)
	}
	if err != nil {
		return nil, fmt.Errorf("error getting spec field %s from reference resource - %w", ref.Field, err)
	}

	for _, item := range all.Items {
		statusMap, ok, err := unstructured.NestedMap(item.Object, "status")
		if !ok {
			return nil, fmt.Errorf("status field not found in reference resource")
		}
		if err != nil {
			return nil, fmt.Errorf("error getting status field from reference resource - %w", err)
		}

		for _, v := range statusMap {
			strField, ok := v.(string)
			if ok {
				if strField == fieldValue {
					return &item, nil
				}
			}
		}

		specMap, ok, err := unstructured.NestedMap(item.Object, "spec")
		if !ok {
			return nil, fmt.Errorf("spec field not found in reference resource")
		}
		if err != nil {
			return nil, fmt.Errorf("error getting spec field from reference resource - %w", err)
		}
		for _, v := range specMap {
			strField, ok := v.(string)
			if ok {
				if strField == fieldValue {
					return &item, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("no reference found for resource %s", gvrForReference.Resource)
}

func isCRUpdated(def getter.Resource, mg *unstructured.Unstructured, rm map[string]interface{}) (bool, error) {
	specs, err := unstructuredtools.GetFieldsFromUnstructured(mg, "spec")
	if err != nil {
		return false, fmt.Errorf("error getting spec fields: %w", err)
	}
	if len(def.CompareList) > 0 {
		for _, field := range def.CompareList {
			if _, ok := rm[field]; !ok {
				return false, fmt.Errorf("field %s not found in response", field)
			}
			if !reflect.DeepEqual(specs[field], rm[field]) {
				return false, nil
			}

		}
		return true, nil
	}

	for k, v := range specs {
		// Skip fields that are not in the response
		if _, ok := rm[k]; !ok {
			// fmt.Printf("skipping field: %s\n", k)
			continue
		}
		if !reflect.DeepEqual(v, rm[k]) {
			return false, nil
		}
	}

	return true, nil
}
