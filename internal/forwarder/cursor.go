package forwarder

import (
	"slices"
	"time"
)

// logCursor remembers the newest timestamp forwarded on a stream and the
// records already forwarded at exactly that timestamp, so a resubscription that
// replays from the cursor skips them without dropping distinct records that
// share the boundary timestamp.
type logCursor struct {
	at   time.Time
	seen map[string]struct{}
}

func newLogCursor(at time.Time) *logCursor {
	return &logCursor{at: at, seen: map[string]struct{}{}}
}

// accept reports whether the record is new, and records it if so.
func (c *logCursor) accept(at time.Time, key string) bool {
	switch {
	case at.Before(c.at):
		return false
	case at.After(c.at):
		c.at = at
		clear(c.seen)
	default:
		if _, ok := c.seen[key]; ok {
			return false
		}
	}
	c.seen[key] = struct{}{}
	return true
}

// sortByTime orders a batch oldest first, so records that arrive out of order
// within one batch are not mistaken for replays.
func sortByTime[T any](items []T, at func(T) time.Time) {
	slices.SortStableFunc(items, func(a, b T) int { return at(a).Compare(at(b)) })
}
