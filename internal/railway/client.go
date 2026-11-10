// Package railway talks to Railway's public GraphQL API: queries over HTTP and
// subscriptions over graphql-transport-ws.
package railway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	DefaultHTTPURL = "https://backboard.railway.com/graphql/v2"
	DefaultWSURL   = "wss://backboard.railway.com/graphql/v2"
)

type Client struct {
	token      string
	httpURL    string
	wsURL      string
	httpClient *http.Client
}

func NewClient(token, httpURL, wsURL string) *Client {
	return &Client{
		token:      token,
		httpURL:    httpURL,
		wsURL:      wsURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type GraphQLError struct {
	Message string `json:"message"`
}

type GraphQLErrors []GraphQLError

func (e GraphQLErrors) Error() string {
	msgs := make([]string, len(e))
	for i, err := range e {
		msgs[i] = err.Message
	}
	return "railway graphql: " + strings.Join(msgs, "; ")
}

// IsNotAuthorized reports whether err is Railway rejecting the token.
func IsNotAuthorized(err error) bool {
	var gqlErrs GraphQLErrors
	if errors.As(err, &gqlErrs) {
		for _, e := range gqlErrs {
			if strings.Contains(strings.ToLower(e.Message), "not authorized") {
				return true
			}
		}
	}
	return false
}

// Query runs a GraphQL query and decodes its data into out.
func (c *Client) Query(ctx context.Context, query string, variables map[string]any, out any) error {
	body, err := json.Marshal(gqlRequest{Query: query, Variables: variables})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors GraphQLErrors   `json:"errors"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("railway graphql: status %d: %w", resp.StatusCode, err)
	}
	if len(envelope.Errors) > 0 {
		return envelope.Errors
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("railway graphql: status %d", resp.StatusCode)
	}
	return json.Unmarshal(envelope.Data, out)
}
