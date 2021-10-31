// Package hasura implements a client for Hasura query and metadata APIs
package hasura

import (
	"net/http"
)

// Client implements the query and metadata client
type Client struct {
	Query *queryClient
}

// NewClient construct new Hasura client
func NewClient(client *http.Client, url string, adminSecret string) *Client {
	if client == nil {
		client = &http.Client{
			Transport: &hasuraTransport{
				rt:          http.DefaultTransport,
				adminSecret: adminSecret,
			},
		}
	} else {
		client.Transport = &hasuraTransport{
			rt:          client.Transport,
			adminSecret: adminSecret,
		}
	}

	return &Client{
		Query: newQueryClient(url, client),
	}
}
