package restclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"fmt"

	"github.com/lucasepe/httplib"
	"github.com/pb33f/libopenapi"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
	orderedmap "github.com/pb33f/libopenapi/orderedmap"

	stringset "github.com/krateoplatformops/composition-dynamic-controller/internal/text"
)

type APICallType string

const (
	APICallsTypeGet  APICallType = "get"
	APICallsTypePost APICallType = "post"
	APICallsTypeList APICallType = "list"
)

func (a APICallType) String() string {
	return string(a)
}
func StringToApiCallType(ty string) (APICallType, error) {
	ty = strings.ToLower(ty)
	switch ty {
	case "get":
		return APICallsTypeGet, nil
	case "post":
		return APICallsTypePost, nil
	case "list":
		return APICallsTypeList, nil
	}
	return "", fmt.Errorf("unknown api call type: %s", ty)
}

type UnstructuredClient struct {
	Server    string
	DocScheme *libopenapi.DocumentModel[v3.Document]
	Auth      httplib.AuthMethod
	Verbose   bool
}

type APIError struct {
	Message   string `json:"message"`
	TypeKey   string `json:"typeKey"`
	ErrorCode int    `json:"errorCode"`
	EventID   int    `json:"eventId"`
}

type RequestConfiguration struct {
	Parameters map[string]string
	Query      map[string]string
	Body       interface{}
}

// type GetRequestConfiguration struct {
// 	RequestConfiguration
// }

// type PostRequestConfiguration struct {
// 	RequestConfiguration
// 	Body interface{}
// }

type AuthType string

const (
	AuthTypeBasic  AuthType = "basic"
	AuthTypeBearer AuthType = "bearer"
)

func (a AuthType) String() string {
	return string(a)
}
func (AuthType) ToType(ty string) (AuthType, error) {
	switch ty {
	case "basic":
		return AuthTypeBasic, nil
	case "bearer":
		return AuthTypeBearer, nil
	}
	return "", fmt.Errorf("unknown auth type: %s", ty)
}

func (e *APIError) Error() string {
	return fmt.Sprintf("error: %s (%s, %d)", e.Message, e.TypeKey, e.EventID)
}

func buildPath(baseUrl string, path string, parameters map[string]string, query map[string]string) *url.URL {
	for key, param := range parameters {
		path = strings.Replace(path, fmt.Sprintf("{%s}", key), fmt.Sprintf("%v", param), 1)
	}

	params := url.Values{}

	for key, param := range query {
		params.Add(key, param)
	}

	parsed, err := url.Parse(baseUrl)
	if err != nil {
		return nil
	}
	parsed.Path = path
	parsed.RawQuery = params.Encode()
	return parsed
}

func getValidResponseCode(codes *orderedmap.Map[string, *v3.Response]) (int, error) {
	for code := codes.First(); code != nil; code = code.Next() {
		icode, err := strconv.Atoi(code.Key())
		if err != nil {
			return 0, fmt.Errorf("invalid response code: %s", code.Key())
		}
		if icode >= 200 && icode < 300 {
			return icode, nil
		}
	}
	return 0, fmt.Errorf("no valid response code found")
}

func (u *UnstructuredClient) ValidateRequest(httpMethod string, path string, parameters map[string]string, query map[string]string) error {
	pathItem, ok := u.DocScheme.Model.Paths.PathItems.Get(path)
	if !ok {
		return fmt.Errorf("path not found: %s", path)
	}
	getDoc, ok := pathItem.GetOperations().Get(strings.ToLower(httpMethod))
	if !ok {
		return fmt.Errorf("operation not found: %s", httpMethod)
	}
	for _, param := range getDoc.Parameters {
		if param.Required != nil && *param.Required {
			if param.In == "path" {
				if _, ok := parameters[param.Name]; !ok {
					return fmt.Errorf("missing path parameter: %s", param.Name)
				}
			}
			if param.In == "query" {
				if _, ok := query[param.Name]; !ok {
					return fmt.Errorf("missing query parameter: %s", param.Name)
				}
			}
		}
	}
	return nil
}

func (u *UnstructuredClient) RequestedBody(httpMethod string, path string) (bodyParams stringset.StringSet, err error) {
	pathItem, ok := u.DocScheme.Model.Paths.PathItems.Get(path)
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	getDoc, ok := pathItem.GetOperations().Get(strings.ToLower(httpMethod))
	if !ok {
		return nil, fmt.Errorf("operation not found: %s", httpMethod)
	}
	bodyParams = stringset.NewStringSet()
	bodySchema, ok := getDoc.RequestBody.Content.Get("application/json")
	if !ok {
		return bodyParams, nil
	}

	for schema := bodySchema.Schema.Schema().Properties.First(); schema != nil; schema = schema.Next() {
		bodyParams.Add(schema.Key())
	}
	// for key, _ := range bodySchema.Properties {
	// 	bodyParams.Add(key)
	// }

	return bodyParams, nil
}

func (u *UnstructuredClient) RequestedParams(httpMethod string, path string) (parameters stringset.StringSet, query stringset.StringSet, err error) {
	pathItem, ok := u.DocScheme.Model.Paths.PathItems.Get(path)
	if !ok {
		return nil, nil, fmt.Errorf("path not found: %s", path)
	}
	getDoc, ok := pathItem.GetOperations().Get(strings.ToLower(httpMethod))
	if !ok {
		return nil, nil, fmt.Errorf("operation not found: %s", httpMethod)
	}
	parameters = stringset.NewStringSet()
	query = stringset.NewStringSet()
	for _, param := range getDoc.Parameters {
		if param.In == "path" {
			parameters.Add(param.Name)
		}
		if param.In == "query" {
			query.Add(param.Name)
		}
	}
	return parameters, query, nil
}

func (u *UnstructuredClient) Get(ctx context.Context, cli *http.Client, path string, opts *RequestConfiguration) (*map[string]interface{}, error) {
	uri := buildPath(u.Server, path, opts.Parameters, opts.Query)

	err := u.ValidateRequest("GET", path, opts.Parameters, opts.Query)
	if err != nil {
		return nil, err
	}
	req, err := httplib.Get(uri.String())
	if err != nil {
		return nil, err
	}

	var val map[string]interface{}
	apiErr := &APIError{}

	httpMethod := "GET"
	pathItem, ok := u.DocScheme.Model.Paths.PathItems.Get(path)
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	getDoc, ok := pathItem.GetOperations().Get(strings.ToLower(httpMethod))
	if !ok {
		return nil, fmt.Errorf("operation not found: %s", httpMethod)
	}

	validStatusCode, err := getValidResponseCode(getDoc.Responses.Codes)
	if err != nil {
		return nil, err
	}

	err = httplib.Fire(cli, req, httplib.FireOptions{
		Verbose:         u.Verbose,
		ResponseHandler: httplib.FromJSON(&val),
		AuthMethod:      u.Auth,
		Validators: []httplib.HandleResponseFunc{
			httplib.ErrorJSON(apiErr, validStatusCode),
		},
	})
	if err != nil {
		return nil, err
	}
	return &val, nil
}

func (u *UnstructuredClient) Post(ctx context.Context, cli *http.Client, path string, opts *RequestConfiguration) (*map[string]interface{}, error) {
	uri := buildPath(u.Server, path, opts.Parameters, opts.Query)

	err := u.ValidateRequest("POST", path, opts.Parameters, opts.Query)
	if err != nil {
		return nil, err
	}

	fmt.Println("POST", opts.Body)

	jBody, err := json.MarshalIndent(opts.Body, "", "  ")
	if err != nil {
		return nil, err
	}
	fmt.Println(string(jBody))

	req, err := httplib.Post(uri.String(), httplib.ToJSON(opts.Body))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	var val map[string]interface{}
	apiErr := &APIError{}

	httpMethod := "POST"
	pathItem, ok := u.DocScheme.Model.Paths.PathItems.Get(path)
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	getDoc, ok := pathItem.GetOperations().Get(strings.ToLower(httpMethod))
	if !ok {
		return nil, fmt.Errorf("operation not found: %s", httpMethod)
	}

	validStatusCode, err := getValidResponseCode(getDoc.Responses.Codes)
	if err != nil {
		return nil, err
	}

	err = httplib.Fire(cli, req, httplib.FireOptions{
		Verbose:         u.Verbose,
		ResponseHandler: httplib.FromJSON(&val),
		AuthMethod:      u.Auth,
		Validators: []httplib.HandleResponseFunc{
			httplib.ErrorJSON(apiErr, validStatusCode),
		},
	})
	if err != nil {
		return nil, err
	}
	return &val, nil
}

// func IsOnList(list map[string]interface{}) (*map[string]interface{}, error) {
// 	for key, val := range list {
// 		if key == "count" {
// 			count := int(val.(float64))
// 			if count > 0 {
// 				return &list, nil
// 			}
// 		}
// 	}

// 	return nil, nil
// }

func (u *UnstructuredClient) List(ctx context.Context, cli *http.Client, path string, opts *RequestConfiguration) (*map[string]interface{}, error) {
	uri := buildPath(u.Server, path, opts.Parameters, opts.Query)

	err := u.ValidateRequest("GET", path, opts.Parameters, opts.Query)
	if err != nil {
		return nil, err
	}
	req, err := httplib.Get(uri.String())
	if err != nil {
		return nil, err
	}

	var val map[string]interface{}
	apiErr := &APIError{}

	httpMethod := "GET"
	pathItem, ok := u.DocScheme.Model.Paths.PathItems.Get(path)
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	getDoc, ok := pathItem.GetOperations().Get(strings.ToLower(httpMethod))
	if !ok {
		return nil, fmt.Errorf("operation not found: %s", httpMethod)
	}

	validStatusCode, err := getValidResponseCode(getDoc.Responses.Codes)
	if err != nil {
		return nil, err
	}

	err = httplib.Fire(cli, req, httplib.FireOptions{
		Verbose:         u.Verbose,
		ResponseHandler: httplib.FromJSON(&val),
		AuthMethod:      u.Auth,
		Validators: []httplib.HandleResponseFunc{
			httplib.ErrorJSON(apiErr, validStatusCode),
		},
	})
	if err != nil {
		return nil, err
	}
	return &val, nil
}

func (u *UnstructuredClient) Delete(ctx context.Context, cli *http.Client, path string, opts *RequestConfiguration) (*map[string]interface{}, error) {
	uri := buildPath(u.Server, path, opts.Parameters, opts.Query)

	err := u.ValidateRequest("DELETE", path, opts.Parameters, opts.Query)
	if err != nil {
		return nil, err
	}
	req, err := httplib.Delete(uri.String())
	if err != nil {
		return nil, err
	}

	var val map[string]interface{}
	apiErr := &APIError{}

	httpMethod := "DELETE"
	pathItem, ok := u.DocScheme.Model.Paths.PathItems.Get(path)
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	getDoc, ok := pathItem.GetOperations().Get(strings.ToLower(httpMethod))
	if !ok {
		return nil, fmt.Errorf("operation not found: %s", httpMethod)
	}

	validStatusCode, err := getValidResponseCode(getDoc.Responses.Codes)
	if err != nil {
		return nil, err
	}

	err = httplib.Fire(cli, req, httplib.FireOptions{
		Verbose:         u.Verbose,
		ResponseHandler: httplib.FromJSON(&val),
		AuthMethod:      u.Auth,
		Validators: []httplib.HandleResponseFunc{
			httplib.ErrorJSON(apiErr, validStatusCode),
		},
	})
	if err != nil {
		return nil, err
	}
	return &val, nil
}

func BuildClient(swaggerPath string) (*UnstructuredClient, error) {

	contents, _ := os.ReadFile(swaggerPath)
	d, err := libopenapi.NewDocument(contents)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	doc, modelErrors := d.BuildV3Model()
	if len(modelErrors) > 0 {
		return nil, fmt.Errorf("failed to build model: %w", errors.Join(modelErrors...))
	}
	if doc == nil {
		return nil, fmt.Errorf("failed to build model")
	}

	// Resolve model references
	resolvingErrors := doc.Index.GetResolver().Resolve()
	errs := []error{}
	for i := range resolvingErrors {
		errs = append(errs, resolvingErrors[i].ErrorRef)
	}
	if len(resolvingErrors) > 0 {
		return nil, fmt.Errorf("failed to resolve model references: %w", errors.Join(errs...))
	}
	if len(doc.Model.Servers) == 0 {
		return nil, fmt.Errorf("no servers found in the document")
	}

	return &UnstructuredClient{
		Server:    doc.Model.Servers[0].URL,
		DocScheme: doc,
		Auth:      nil,
	}, nil
}
