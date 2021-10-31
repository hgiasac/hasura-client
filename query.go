package hasura

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// QueryResultType run_sql result type enum
type QueryResultType string

const (
	CommandOk QueryResultType = "CommandOk"
	TuplesOk  QueryResultType = "TuplesOk"
)

// QueryResult represents query result response
type QueryResult struct {
	ResultType QueryResultType `json:"result_type"`
	Result     [][]string      `json:"result"`
}

type queryClient struct {
	url    string
	client *http.Client
}

func newQueryClient(baseURL string, client *http.Client) *queryClient {
	return &queryClient{
		url:    fmt.Sprintf("%s/v2/query", baseURL),
		client: client,
	}
}

// Send sends request to query resources
func (c *queryClient) Send(body interface{}) (io.ReadCloser, error) {

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	var reqBody = new(bytes.Buffer)
	_, err = reqBody.Write(jsonBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Post(c.url, "application/json", reqBody)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 200 {
		return resp.Body, nil
	}

	// decode and return error if the status isn't 200
	respBody, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}
	return nil, errors.New(string(respBody))
}

// Bulk sends multiple query requests at once
func (c *queryClient) Bulk(args []*RequestBody) (io.ReadCloser, error) {
	body := RequestBody{
		Type: "bulk",
		Args: args,
	}

	return c.Send(body)
}

// RunSQL sends executing SQL request to a database source
func (c *queryClient) RunSQL(source string, sql string, cascade bool) (io.ReadCloser, error) {
	requestBody := NewRunSQLRequest(source, sql, cascade)
	return c.Send(requestBody)
}

// QueryArgs represent request query arguments
type QueryArgs struct {
	Cascade  bool   `json:"cascade"`
	Source   string `json:"source"`
	SQL      string `json:"sql"`
	ReadOnly bool   `json:"read_only"`
}

// NewRunSQLRequest create a run_sql request
func NewRunSQLRequest(source string, sql string, cascade bool) *RequestBody {
	return &RequestBody{
		Type: "run_sql",
		Args: QueryArgs{
			Source:  source,
			SQL:     sql,
			Cascade: cascade,
		},
	}
}
