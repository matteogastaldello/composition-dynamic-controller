package genericclient

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"fmt"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/lucasepe/httplib"
)

type UnstructuredClient struct {
	Server    string
	DocScheme *openapi3.T
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
}

type GetRequestConfiguration struct {
	RequestConfiguration
}

type PostRequestConfiguration struct {
	RequestConfiguration
	Body interface{}
}

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
	return fmt.Sprintf("azuredevops: %s (%s, %d)", e.Message, e.TypeKey, e.EventID)
}

func buildPath(baseUrl string, path string, parameters map[string]string, query map[string]string) url.URL {
	for key, param := range parameters {
		path = strings.Replace(path, fmt.Sprintf("{%s}", key), fmt.Sprintf("%v", param), 1)
	}

	params := url.Values{}

	for key, param := range query {
		params.Add(key, param)
	}
	uri := url.URL{
		Scheme:   "https",
		Host:     strings.Split(baseUrl, "//")[1],
		Path:     path,
		RawQuery: params.Encode(),
	}
	return uri
}

func getValidResponseCode(codes map[string]*openapi3.ResponseRef) (int, error) {
	for key, _ := range codes {
		icode, err := strconv.Atoi(key)
		if err != nil {
			return 0, fmt.Errorf("invalid response code: %s", key)
		}
		if icode >= 200 && icode < 300 {
			return icode, nil
		}
	}
	return 0, fmt.Errorf("no valid response code found")
}

func (u *UnstructuredClient) ValidateRequest(httpMethod string, path string, parameters map[string]string, query map[string]string) error {

	getDoc := u.DocScheme.Paths.Find(path).GetOperation(httpMethod)
	for _, param := range getDoc.Parameters {
		if param.Value.In == "path" {
			if _, ok := parameters[param.Value.Name]; !ok {
				return fmt.Errorf("missing path parameter: %s", param.Value.Name)
			}
		}
		if param.Value.In == "query" {
			if _, ok := query[param.Value.Name]; !ok {
				return fmt.Errorf("missing query parameter: %s", param.Value.Name)
			}
		}
	}
	return nil
}

func (u *UnstructuredClient) Get(ctx context.Context, cli *http.Client, path string, opts *GetRequestConfiguration) (*map[string]interface{}, error) {
	uri := buildPath(u.Server, path, opts.Parameters, opts.Query)

	err := u.ValidateRequest("GET", path, opts.Parameters, opts.Query)
	if err != nil {
		panic(err)
	}
	req, err := httplib.Get(uri.String())
	if err != nil {
		return nil, err
	}

	var val map[string]interface{}
	apiErr := &APIError{}

	validStatusCode, err := getValidResponseCode(u.DocScheme.Paths.Find(path).GetOperation("GET").Responses.Map())
	if err != nil {
		panic(err)
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
		panic(err)
	}
	return &val, nil
}

func (u *UnstructuredClient) Post(ctx context.Context, cli *http.Client, path string, opts *PostRequestConfiguration) (*map[string]interface{}, error) {
	uri := buildPath(u.Server, path, opts.Parameters, opts.Query)

	err := u.ValidateRequest("POST", path, opts.Parameters, opts.Query)
	if err != nil {
		panic(err)
	}
	req, err := httplib.Post(uri.String(), httplib.ToJSON(opts.Body))
	if err != nil {
		return nil, err
	}

	var val map[string]interface{}
	apiErr := &APIError{}

	validStatusCode, err := getValidResponseCode(u.DocScheme.Paths.Find(path).GetOperation("POST").Responses.Map())
	if err != nil {
		panic(err)
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
		panic(err)
	}
	return &val, nil
}

func (u *UnstructuredClient) List(ctx context.Context, cli *http.Client, path string, opts *GetRequestConfiguration) (*map[string]interface{}, error) {
	uri := buildPath(u.Server, path, opts.Parameters, opts.Query)

	err := u.ValidateRequest("GET", path, opts.Parameters, opts.Query)
	if err != nil {
		panic(err)
	}
	req, err := httplib.Get(uri.String())
	if err != nil {
		return nil, err
	}

	var val map[string]interface{}
	apiErr := &APIError{}

	validStatusCode, err := getValidResponseCode(u.DocScheme.Paths.Find(path).GetOperation("GET").Responses.Map())
	if err != nil {
		panic(err)
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
		panic(err)
	}
	return &val, nil
}

func BuildClient(swaggerPath string) (*UnstructuredClient, error) {
	// Load the OpenAPI 3.0 spec file
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(swaggerPath)
	if err != nil {
		return nil, err
	}

	// Validate the loaded OpenAPI spec
	if err = doc.Validate(context.Background()); err != nil {
		return nil, err
	}

	// Get the first server URL
	var serverUrl string
	for _, server := range doc.Servers {
		if server.URL != "" {
			serverUrl = server.URL
			break
		}
	}
	if serverUrl == "" {
		return nil, fmt.Errorf("no server URL found")
	}

	return &UnstructuredClient{
		Server:    serverUrl,
		DocScheme: doc,
		Auth:      nil,
	}, nil
}
