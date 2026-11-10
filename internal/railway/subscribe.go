package railway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

const (
	handshakeTimeout = 10 * time.Second
	pingInterval     = 30 * time.Second
	readLimit        = 64 << 20
)

// opens limits concurrent WebSocket handshakes; Railway rate-limits bursts of them.
var opens = make(chan struct{}, 3)

type wsMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// ErrSubscriptionComplete is returned when the server ends a subscription.
var ErrSubscriptionComplete = errors.New("railway subscription completed by server")

// Subscribe opens one graphql-transport-ws subscription and calls onData with the
// `data` object of every `next` message. It returns when the context ends, the
// connection drops, the server completes the subscription, or onData fails.
func (c *Client) Subscribe(ctx context.Context, query string, variables map[string]any, onData func(json.RawMessage) error) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	payload, err := json.Marshal(gqlRequest{Query: query, Variables: variables})
	if err != nil {
		return err
	}
	if err := writeJSON(ctx, conn, wsMessage{ID: "1", Type: "subscribe", Payload: payload}); err != nil {
		return fmt.Errorf("railway subscribe: %w", err)
	}

	parent := ctx
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go keepAlive(ctx, cancel, conn)

	for {
		msg, err := readMessage(ctx, conn)
		if err != nil {
			if parent.Err() != nil {
				return parent.Err()
			}
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			return fmt.Errorf("railway subscription read: %w", err)
		}
		switch msg.Type {
		case "next":
			var next struct {
				Data   json.RawMessage `json:"data"`
				Errors GraphQLErrors   `json:"errors"`
			}
			if err := json.Unmarshal(msg.Payload, &next); err != nil {
				return fmt.Errorf("railway subscription payload: %w", err)
			}
			if len(next.Errors) > 0 {
				return next.Errors
			}
			if err := onData(next.Data); err != nil {
				return err
			}
		case "error":
			var errs GraphQLErrors
			if json.Unmarshal(msg.Payload, &errs) == nil && len(errs) > 0 {
				return errs
			}
			return fmt.Errorf("railway subscription error: %s", string(msg.Payload))
		case "complete":
			conn.Close(websocket.StatusNormalClosure, "")
			return ErrSubscriptionComplete
		case "ping":
			if err := writeJSON(ctx, conn, wsMessage{Type: "pong"}); err != nil {
				return err
			}
		}
	}
}

func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	select {
	case opens <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-opens }()

	hctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	header := http.Header{}
	header.Set("Authorization", "Bearer "+c.token)
	conn, _, err := websocket.Dial(hctx, c.wsURL, &websocket.DialOptions{
		HTTPHeader:   header,
		Subprotocols: []string{"graphql-transport-ws"},
	})
	if err != nil {
		return nil, fmt.Errorf("railway websocket dial: %w", err)
	}
	conn.SetReadLimit(readLimit)

	if err := writeJSON(hctx, conn, wsMessage{Type: "connection_init"}); err != nil {
		conn.CloseNow()
		return nil, fmt.Errorf("railway connection_init: %w", err)
	}
	for {
		msg, err := readMessage(hctx, conn)
		if err != nil {
			conn.CloseNow()
			return nil, fmt.Errorf("railway connection_ack: %w", err)
		}
		switch msg.Type {
		case "connection_ack":
			return conn, nil
		case "ping":
			if err := writeJSON(hctx, conn, wsMessage{Type: "pong"}); err != nil {
				conn.CloseNow()
				return nil, err
			}
		default:
			conn.CloseNow()
			return nil, fmt.Errorf("railway handshake: unexpected %q message: %s", msg.Type, string(msg.Payload))
		}
	}
}

// keepAlive sends WebSocket pings so a silently dead connection is noticed.
// coder/websocket processes pongs inside the reader, which Subscribe runs.
func keepAlive(ctx context.Context, cancel context.CancelCauseFunc, conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pctx, pcancel := context.WithTimeout(ctx, handshakeTimeout)
			err := conn.Ping(pctx)
			pcancel()
			if err != nil && ctx.Err() == nil {
				cancel(fmt.Errorf("railway websocket ping: %w", err))
				conn.CloseNow()
				return
			}
		}
	}
}

func writeJSON(ctx context.Context, conn *websocket.Conn, msg wsMessage) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, b)
}

func readMessage(ctx context.Context, conn *websocket.Conn) (wsMessage, error) {
	_, b, err := conn.Read(ctx)
	if err != nil {
		return wsMessage{}, err
	}
	var msg wsMessage
	if err := json.Unmarshal(b, &msg); err != nil {
		return wsMessage{}, fmt.Errorf("decode websocket message: %w", err)
	}
	return msg, nil
}
