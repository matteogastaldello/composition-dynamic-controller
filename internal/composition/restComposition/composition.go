package composition

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/client/restclient"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/controller"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/meta"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/text"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools/apiaction"
	getter "github.com/krateoplatformops/composition-dynamic-controller/internal/tools/restclient"
	"github.com/lucasepe/httplib"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools"
	unstructuredtools "github.com/krateoplatformops/composition-dynamic-controller/internal/tools/unstructured"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var _ controller.ExternalClient = (*handler)(nil)

func NewHandler(cfg *rest.Config, log *zerolog.Logger, swg getter.Getter) controller.ExternalClient {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Creating dynamic client.")
	}

	dis, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Creating discovery client.")
	}

	return &handler{
		logger:            log,
		dynamicClient:     dyn,
		discoveryClient:   dis,
		swaggerInfoGetter: swg,
	}
}

type handler struct {
	logger              *zerolog.Logger
	dynamicClient       dynamic.Interface
	discoveryClient     *discovery.DiscoveryClient
	openapiFileLocation string
	swaggerInfoGetter   getter.Getter
}

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

func (h *handler) Observe(ctx context.Context, mg *unstructured.Unstructured) (bool, error) {
	log := h.logger.With().Timestamp().
		Str("op", "Observe").
		Str("apiVersion", mg.GetAPIVersion()).
		Str("kind", mg.GetKind()).
		Str("name", mg.GetName()).
		Str("namespace", mg.GetNamespace()).Logger()

	if h.swaggerInfoGetter == nil {
		return false, fmt.Errorf("swagger file info getter must be specified")
	}

	clientInfo, err := h.swaggerInfoGetter.Get(mg)
	if err != nil {
		log.Err(err).Msg("Getting REST client info")
		return false, err
	}

	cli, err := restclient.BuildClient(clientInfo.URL)
	if err != nil {
		log.Err(err).Msg("Building REST client")
		return false, err
	}
	cli.Auth = clientInfo.Auth
	cli.Verbose = meta.IsVerbose(mg)

	specFields, err := unstructuredtools.GetFieldsFromUnstructured(mg, "spec")
	if err != nil {
		log.Err(err).Msg("Getting spec")
		return false, err
	}
	statusFields, err := unstructuredtools.GetFieldsFromUnstructured(mg, "status")
	if err == fmt.Errorf("%s not found", "status") {
		log.Debug().Str("Resource", mg.GetKind()).Msg("External resource not created yet.")
		return false, nil
	}
	if err != nil {
		log.Err(err).Msg("Getting status")
		return false, nil
	}
	// If status is empty, the resource is not created yet.
	if statusFields[clientInfo.Resource.Identifier] == nil {
		log.Debug().Str("Resource", mg.GetKind()).Msg("External resource not created yet.")
		return false, nil
	}

	apiCall, callInfo, err := APICallBuilder(cli, clientInfo, apiaction.Get)
	if err != nil {
		log.Err(err).Msg("Building API call")
		return false, err
	}
	reqConfiguration := BuildCallConfig(callInfo, statusFields, specFields)
	if reqConfiguration == nil {
		return false, fmt.Errorf("error building call configuration")
	}
	body, err := apiCall(ctx, http.DefaultClient, callInfo.Path, reqConfiguration)
	if httplib.IsNotFoundError(err) {
		log.Debug().Str("Resource", mg.GetKind()).Msg("External resource not found.")
		return false, nil
	}
	if err != nil {
		log.Err(err).Msg("Performing REST call")
		return false, err
	}
	if body == nil {
		return false, fmt.Errorf("response body is nil")
	}
	for k, v := range *body {
		if k == callInfo.IdentifierField {
			err = unstructured.SetNestedField(mg.Object, text.GenericToString(v), "status", callInfo.IdentifierField)
			if err != nil {
				log.Err(err).Msg("Setting identifier")
				return false, err
			}
		}
	}

	err = tools.UpdateStatus(ctx, mg, tools.UpdateOptions{
		DiscoveryClient: h.discoveryClient,
		DynamicClient:   h.dynamicClient,
	})
	if err != nil {
		log.Err(err).Msg("Updating status")
		return false, err
	}

	log.Debug().Str("Resource", mg.GetKind()).Msg("External resource up-to-date.")

	return true, nil // apierrors.NewNotFound(schema.GroupResource{
	//		Group:    mg.GroupVersionKind().Group,
	//		Resource: flect.Pluralize(strings.ToLower(mg.GetKind())),
	//	}, mg.GetName())
}

func (h *handler) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	log := h.logger.With().Timestamp().
		Str("op", "Create").
		Str("apiVersion", mg.GetAPIVersion()).
		Str("kind", mg.GetKind()).
		Str("name", mg.GetName()).
		Str("namespace", mg.GetNamespace()).Logger()

	if h.swaggerInfoGetter == nil {
		return fmt.Errorf("swagger info getter must be specified")
	}

	clientInfo, err := h.swaggerInfoGetter.Get(mg)
	if err != nil {
		log.Err(err).Msg("Getting REST client info")
		return err
	}

	cli, err := restclient.BuildClient(clientInfo.URL)
	if err != nil {
		log.Err(err).Msg("Building REST client")
		return err
	}
	cli.Auth = clientInfo.Auth
	cli.Verbose = meta.IsVerbose(mg)

	specFields, err := unstructuredtools.GetFieldsFromUnstructured(mg, "spec")
	if err != nil {
		log.Err(err).Msg("Getting spec")
		return err
	}
	apiCall, callInfo, err := APICallBuilder(cli, clientInfo, apiaction.Create)
	if err != nil {
		log.Err(err).Msg("Building API call")
		return err
	}
	reqConfiguration := BuildCallConfig(callInfo, nil, specFields)
	body, err := apiCall(ctx, http.DefaultClient, callInfo.Path, reqConfiguration)
	if err != nil {
		log.Err(err).Msg("Performing REST call")
		return err
	}
	if body == nil {
		return fmt.Errorf("response body is nil")
	}
	for k, v := range *body {
		if k == callInfo.IdentifierField {
			err = unstructured.SetNestedField(mg.Object, text.GenericToString(v), "status", callInfo.IdentifierField)
			if err != nil {
				log.Err(err).Msg("Setting identifier")
				return err
			}
		}
	}

	log.Debug().Str("Resource", mg.GetKind()).Msg("Creating external resource.")

	err = tools.UpdateStatus(ctx, mg, tools.UpdateOptions{
		DiscoveryClient: h.discoveryClient,
		DynamicClient:   h.dynamicClient,
	})
	if err != nil {
		log.Err(err).Msg("Updating status")
		return err
	}

	return nil
}

func (h *handler) Update(ctx context.Context, mg *unstructured.Unstructured) error {
	// log := h.logger.With().
	// 	Str("op", "Update").
	// 	Str("apiVersion", mg.GetAPIVersion()).
	// 	Str("kind", mg.GetKind()).
	// 	Str("name", mg.GetName()).
	// 	Str("namespace", mg.GetNamespace()).Logger()

	// log.Debug().Msg("Handling composition values update.")

	// // If we started but never completed creation of an external resource we
	// // may have lost critical information.The safest thing to
	// // do is to refuse to proceed.
	// if meta.ExternalCreateIncomplete(mg) {
	// 	log.Warn().Msg(errCreateIncomplete)
	// 	_ = unstructuredtools.SetCondition(mg, condition.Creating())

	// 	return tools.UpdateStatus(ctx, mg, tools.UpdateOptions{
	// 		DiscoveryClient: h.discoveryClient,
	// 		DynamicClient:   h.dynamicClient,
	// 	})
	// }

	// meta.SetExternalCreatePending(mg, time.Now())
	// err := tools.Update(ctx, mg, tools.UpdateOptions{
	// 	DiscoveryClient: h.discoveryClient,
	// 	DynamicClient:   h.dynamicClient,
	// })
	// if err != nil {
	// 	log.Err(err).Msg("Setting meta create pending annotation.")
	// 	return err
	// }

	// if h.packageInfoGetter == nil {
	// 	return fmt.Errorf("helm chart package info getter must be specified")
	// }

	// hc, err := h.helmClientForResource(mg)
	// if err != nil {
	// 	log.Err(err).Msg("Getting helm client")
	// 	return err
	// }

	// pkg, err := h.packageInfoGetter.Get(mg)
	// if err != nil {
	// 	log.Err(err).Msg("Getting package info")
	// 	return err
	// }

	// err = helmchart.Update(ctx, helmchart.UpdateOptions{
	// 	HelmClient: hc,
	// 	ChartName:  pkg.URL,
	// 	Resource:   mg,
	// })
	// if err != nil {
	// 	log.Err(err).Msg("Performing helm chart update")
	// 	return err
	// }

	// log.Debug().Str("package", pkg.URL).Msg("Composition values updated.")

	return nil
}

func (h *handler) Delete(ctx context.Context, mg *unstructured.Unstructured) error {
	if h.swaggerInfoGetter == nil {
		return fmt.Errorf("swagger info getter must be specified")
	}

	clientInfo, err := h.swaggerInfoGetter.Get(mg)
	if err != nil {
		log.Err(err).Msg("Getting REST client info")
		return err
	}

	cli, err := restclient.BuildClient(clientInfo.URL)
	if err != nil {
		log.Err(err).Msg("Building REST client")
		return err
	}
	cli.Auth = clientInfo.Auth
	cli.Verbose = true

	specFields, err := unstructuredtools.GetFieldsFromUnstructured(mg, "spec")
	if err != nil {
		log.Err(err).Msg("Getting spec")
		return err
	}
	statusFields, err := unstructuredtools.GetFieldsFromUnstructured(mg, "status")
	if err != nil {
		log.Err(err).Msg("Getting status")
		return err
	}
	apiCall, callInfo, err := APICallBuilder(cli, clientInfo, apiaction.Delete)
	if err != nil {
		log.Err(err).Msg("Building API call")
		return err
	}
	reqConfiguration := BuildCallConfig(callInfo, statusFields, specFields)
	if reqConfiguration == nil {
		return fmt.Errorf("error building call configuration")
	}
	// reqConfiguration := &restclient.RequestConfiguration{}
	// reqConfiguration.Parameters = make(map[string]string)
	// reqConfiguration.Query = make(map[string]string)
	// mapBody := make(map[string]interface{})
	// for field, value := range specFields {
	// 	if callInfo.ReqParams.Parameters.Contains(field) {
	// 		stringVal := fmt.Sprintf("%v", value)
	// 		reqConfiguration.Parameters[field] = stringVal
	// 	} else if callInfo.ReqParams.Query.Contains(field) {
	// 		stringVal := fmt.Sprintf("%v", value)
	// 		reqConfiguration.Query[field] = stringVal
	// 	} else if callInfo.ReqParams.Body.Contains(field) {
	// 		stringVal := fmt.Sprintf("%v", value)
	// 		mapBody[field] = stringVal
	// 	}
	// }
	body, err := apiCall(ctx, http.DefaultClient, callInfo.Path, reqConfiguration)
	if err != nil {
		log.Err(err).Msg("Performing REST call")
		return err
	}
	if body == nil {
		return fmt.Errorf("response body is nil")
	}
	for k, v := range *body {
		if k == callInfo.IdentifierField {
			err = unstructured.SetNestedField(mg.Object, v, "status", callInfo.IdentifierField)
			if err != nil {
				log.Err(err).Msg("Setting identifier")
				return err
			}
		}
	}

	log.Debug().Str("Resource", mg.GetKind()).Msg("Creating external resource.")

	err = tools.UpdateStatus(ctx, mg, tools.UpdateOptions{
		DiscoveryClient: h.discoveryClient,
		DynamicClient:   h.dynamicClient,
	})
	if err != nil {
		log.Err(err).Msg("Updating status")
		return err
	}

	return nil
}
