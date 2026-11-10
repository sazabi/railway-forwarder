package forwarder

import (
	"testing"
	"time"
)

func TestLogCursorKeepsDistinctRecordsAtTheBoundary(t *testing.T) {
	start := time.Now()
	at := start.Add(time.Second)
	c := newLogCursor(start)
	if !c.accept(at, "a") || !c.accept(at, "b") {
		t.Fatal("distinct records sharing a timestamp were dropped")
	}
	if c.accept(at, "a") {
		t.Fatal("a replayed record was accepted")
	}
	if c.accept(start.Add(-time.Second), "c") {
		t.Fatal("a record older than the cursor was accepted")
	}
	if !c.accept(at.Add(time.Millisecond), "a") {
		t.Fatal("a newer record was dropped")
	}
}

func TestSortByTimeOrdersOutOfOrderBatches(t *testing.T) {
	now := time.Now()
	logs := []httpLog{{RequestID: "late", Timestamp: now.Add(2 * time.Second)}, {RequestID: "early", Timestamp: now.Add(time.Second)}}
	sortByTime(logs, func(l httpLog) time.Time { return l.Timestamp })
	c := newLogCursor(now)
	for _, l := range logs {
		if !c.accept(l.Timestamp, l.RequestID) {
			t.Fatalf("%s was dropped", l.RequestID)
		}
	}
}
