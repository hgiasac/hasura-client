package hasura

import "net/http"

// hasuraTransport transport for Hasura Client
type hasuraTransport struct {
	adminSecret string
	// keep a reference to the client's original transport
	rt http.RoundTripper
}

// RoundTrip set header data before executing http request
func (t *hasuraTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.adminSecret != "" {
		r.Header.Set("X-Hasura-Admin-Secret", t.adminSecret)
	}

	return t.rt.RoundTrip(r)
}

// RequestBody the general request body for both query and metadata APIs
type RequestBody struct {
	Type    string      `json:"type"`
	Version uint        `json:"version,omitempty"`
	Args    interface{} `json:"args"`
}
