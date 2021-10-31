//go:build integration
// +build integration

package hasura

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestQueryClient(t *testing.T) {
	adminSecret := os.Getenv("HASURA_GRAPHQL_ADMIN_SECRET")
	baseURL := os.Getenv("HASURA_BASE_URL")

	client := NewClient(
		&http.Client{
			Transport: http.DefaultTransport,
			Timeout:   30 * time.Second,
		},
		baseURL,
		adminSecret,
	)

	queryURL := fmt.Sprintf("%s/v2/query", baseURL)
	if client.Query.url != queryURL {
		t.Errorf("invalid query url. Expected: %s, got: %s", queryURL, client.Query.url)
		return
	}

	reader, err := client.Query.RunSQL("default", "WITH \"test\" AS (SELECT unnest(ARRAY[0,1,2]) AS id) SELECT id FROM \"test\"", false)
	if err != nil {
		t.Errorf("run_sql failed: %s", err)
		return
	}

	var result QueryResult
	err = json.NewDecoder(reader).Decode(&result)

	if err != nil {
		t.Errorf("decode run_sql response failed: %s", err)
		return
	}

	if result.ResultType != TuplesOk {
		t.Errorf("decode run_sql response failed. Expected result type %s, got %s", TuplesOk, result.ResultType)
		return
	}
	if len(result.Result) != 4 {
		t.Errorf("decode run_sql response failed. Expected 2 rows, got %d", len(result.Result))
		return
	}
	if len(result.Result[0]) != 1 {
		t.Errorf("decode run_sql response failed. Expected 1 column, got %d", len(result.Result[0]))
		return
	}
}
