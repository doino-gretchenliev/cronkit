package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestCapWriter(t *testing.T) {
	var buf bytes.Buffer
	cw := newCapWriter(&buf, 10)
	if _, err := cw.Write([]byte("0123456789ABCDEF")); err != nil {
		t.Fatal(err)
	}
	s := buf.String()
	if !strings.HasPrefix(s, "0123456789") {
		t.Fatalf("want first 10 bytes kept, got %q", s)
	}
	if !strings.Contains(s, "truncated") {
		t.Fatalf("want truncation notice, got %q", s)
	}
	before := buf.Len()
	if _, err := cw.Write([]byte("more")); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != before {
		t.Errorf("data written after cap: grew by %d", buf.Len()-before)
	}
}

func TestCapWriterUnlimited(t *testing.T) {
	var buf bytes.Buffer
	cw := newCapWriter(&buf, 0)
	cw.Write([]byte("hello world"))
	if buf.String() != "hello world" {
		t.Errorf("unlimited cap altered output: %q", buf.String())
	}
}

func TestTimestampWriter(t *testing.T) {
	var buf bytes.Buffer
	tw := newTimestampWriter(&buf, time.UTC)
	tw.Write([]byte("hello\nworld\n"))
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d: %q", len(lines), buf.String())
	}
	for _, ln := range lines {
		if len(ln) < 5 || ln[4] != '-' { // ISO year then '-'
			t.Errorf("line missing timestamp prefix: %q", ln)
		}
	}
}
