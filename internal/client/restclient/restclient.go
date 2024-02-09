package restclient

import (
	"context"
	"net/http"
	"strings"

	"fmt"

	"github.com/lucasepe/httplib"
	"github.com/pb33f/libopenapi"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
)

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

	// jBody, err := json.MarshalIndent(opts.Body, "", "  ")
	// if err != nil {
	// 	return nil, err
	// }
	// fmt.Println(string(jBody))

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
