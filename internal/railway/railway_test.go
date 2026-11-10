package railway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func wsServer(t *testing.T, handle func(ctx context.Context, conn *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q", got)
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"graphql-transport-ws"}})
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		if conn.Subprotocol() != "graphql-transport-ws" {
			t.Errorf("subprotocol = %q", conn.Subprotocol())
		}
		handle(r.Context(), conn)
	}))
}

func expect(t *testing.T, ctx context.Context, conn *websocket.Conn, typ string) wsMessage {
	t.Helper()
	msg, err := readMessage(ctx, conn)
	if err != nil {
		t.Fatalf("read %s: %v", typ, err)
	}
	if msg.Type != typ {
		t.Fatalf("got %q, want %q", msg.Type, typ)
	}
	return msg
}

func send(ctx context.Context, conn *websocket.Conn, msg string) {
	_ = conn.Write(ctx, websocket.MessageText, []byte(msg))
}

func TestSubscribeProtocol(t *testing.T) {
	srv := wsServer(t, func(ctx context.Context, conn *websocket.Conn) {
		expect(t, ctx, conn, "connection_init")
		send(ctx, conn, `{"type":"ping"}`)
		expect(t, ctx, conn, "pong")
		send(ctx, conn, `{"type":"connection_ack"}`)
		sub := expect(t, ctx, conn, "subscribe")
		var p gqlRequest
		_ = json.Unmarshal(sub.Payload, &p)
		if !strings.Contains(p.Query, "environmentLogs") || p.Variables["environmentId"] != "env" {
			t.Errorf("unexpected subscribe payload %s", sub.Payload)
		}
		send(ctx, conn, `{"id":"1","type":"next","payload":{"data":{"n":1}}}`)
		send(ctx, conn, `{"type":"ping"}`)
		expect(t, ctx, conn, "pong")
		send(ctx, conn, `{"id":"1","type":"next","payload":{"data":{"n":2}}}`)
		send(ctx, conn, `{"id":"1","type":"complete"}`)
		_, _, _ = conn.Read(ctx)
	})
	defer srv.Close()

	c := NewClient(ClientOptions{Token: "tok", HTTPURL: srv.URL, WSURL: "ws" + strings.TrimPrefix(srv.URL, "http")})
	var got []string
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.Subscribe(ctx, "subscription { environmentLogs }", map[string]any{"environmentId": "env"}, func(d json.RawMessage) error {
		got = append(got, string(d))
		return nil
	})
	if !errors.Is(err, ErrSubscriptionComplete) {
		t.Fatalf("err = %v", err)
	}
	if strings.Join(got, ",") != `{"n":1},{"n":2}` {
		t.Errorf("data = %v", got)
	}
}

func TestSubscribeSurfacesGraphQLErrors(t *testing.T) {
	srv := wsServer(t, func(ctx context.Context, conn *websocket.Conn) {
		expect(t, ctx, conn, "connection_init")
		send(ctx, conn, `{"type":"connection_ack"}`)
		expect(t, ctx, conn, "subscribe")
		send(ctx, conn, `{"id":"1","type":"error","payload":[{"message":"Not Authorized"}]}`)
		_, _, _ = conn.Read(ctx)
	})
	defer srv.Close()
	c := NewClient(ClientOptions{Token: "tok", HTTPURL: srv.URL, WSURL: "ws" + strings.TrimPrefix(srv.URL, "http")})
	err := c.Subscribe(context.Background(), "subscription { x }", nil, func(json.RawMessage) error { return nil })
	if !IsNotAuthorized(err) {
		t.Fatalf("err = %v, want not authorized", err)
	}
}

func TestFetchEnvironment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req gqlRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch {
		case strings.Contains(req.Query, "forwarderEnvironment"):
			_, _ = w.Write([]byte(`{"data":{"environment":{"id":"env","projectId":"proj",
				"deployments":{"edges":[
					{"node":{"id":"d-web","serviceId":"web","status":"SUCCESS"}},
					{"node":{"id":"d-web-old","serviceId":"web","status":"REMOVED"}},
					{"node":{"id":"d-worker","serviceId":"worker","status":"SUCCESS"}},
					{"node":{"id":"d-api","serviceId":"api","status":"SUCCESS"}}]},
				"serviceInstances":{"edges":[
					{"node":{"serviceId":"web","domains":{"serviceDomains":[{"domain":"web.up.railway.app"}],"customDomains":[]}}},
					{"node":{"serviceId":"api","domains":{"serviceDomains":[],"customDomains":[{"domain":"api.example.com"}]}}},
					{"node":{"serviceId":"worker","domains":{"serviceDomains":[],"customDomains":[]}}}]}}}}`))
		case strings.Contains(req.Query, "forwarderProject"):
			_, _ = w.Write([]byte(`{"data":{"project":{"id":"proj","name":"shop",
				"environments":{"edges":[{"node":{"id":"other","name":"staging"}},{"node":{"id":"env","name":"production"}}]},
				"services":{"edges":[{"node":{"id":"web","name":"Web"}},{"node":{"id":"api","name":"API"}},{"node":{"id":"worker","name":"Worker"}}]}}}}`))
		default:
			t.Errorf("unexpected query %s", req.Query)
		}
	}))
	defer srv.Close()
	env, err := NewClient(ClientOptions{Token: "tok", HTTPURL: srv.URL}).FetchEnvironment(context.Background(), "env")
	if err != nil {
		t.Fatal(err)
	}
	if env.Name != "production" || env.ProjectName != "shop" || env.Services["worker"] != "Worker" {
		t.Errorf("env = %+v", env)
	}
	if len(env.HTTPDeployments) != 2 || env.HTTPDeployments["d-web"] != "web" || env.HTTPDeployments["d-api"] != "api" {
		t.Errorf("http deployments = %v", env.HTTPDeployments)
	}
}

func TestQueryErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"errors":[{"message":"Not Authorized"}],"data":null}`))
	}))
	defer srv.Close()
	_, err := NewClient(ClientOptions{Token: "tok", HTTPURL: srv.URL}).FetchEnvironment(context.Background(), "env")
	if !IsNotAuthorized(err) {
		t.Fatalf("err = %v", err)
	}
}
