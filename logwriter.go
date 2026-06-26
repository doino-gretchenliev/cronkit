package main

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"
)

// capWriter caps total bytes written to the underlying writer. Once the limit
// is reached it writes a one-time truncation notice and discards the rest
// (reporting success so the running command isn't disrupted). limit <= 0 is
// unlimited.
type capWriter struct {
	w         io.Writer
	limit     int64
	written   int64
	truncated bool
}

func newCapWriter(w io.Writer, limit int64) *capWriter {
	return &capWriter{w: w, limit: limit}
}

func (c *capWriter) Write(p []byte) (int, error) {
	if c.limit <= 0 || c.written < c.limit {
		remain := int64(len(p))
		if c.limit > 0 {
			remain = c.limit - c.written
		}
		if int64(len(p)) <= remain {
			n, err := c.w.Write(p)
			c.written += int64(n)
			return n, err
		}
		if _, err := c.w.Write(p[:remain]); err != nil {
			return int(remain), err
		}
		c.written += remain
	}
	if !c.truncated {
		c.truncated = true
		fmt.Fprintf(c.w, "\n... [output truncated at %d bytes] ...\n", c.limit)
	}
	return len(p), nil // pretend fully consumed
}

// tsLayout is ISO 8601 with milliseconds and the local UTC offset, e.g.
// 2026-06-26T23:04:11.402+03:00.
const tsLayout = "2006-01-02T15:04:05.000Z07:00"

// timestampWriter prefixes every output line with an ISO-8601 timestamp. Both a
// job's stdout and stderr are pointed at one of these, so the mutex also
// serializes the two streams (no torn/interleaved lines). The timestamp marks
// when cronkit received the line, not when the program produced it.
type timestampWriter struct {
	mu          sync.Mutex
	w           io.Writer
	loc         *time.Location
	atLineStart bool
}

func newTimestampWriter(w io.Writer, loc *time.Location) *timestampWriter {
	if loc == nil {
		loc = time.Local
	}
	return &timestampWriter{w: w, loc: loc, atLineStart: true}
}

func (tw *timestampWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	consumed := 0
	for consumed < len(p) {
		if tw.atLineStart {
			if _, err := io.WriteString(tw.w, time.Now().In(tw.loc).Format(tsLayout)+"  "); err != nil {
				return consumed, err
			}
			tw.atLineStart = false
		}
		rest := p[consumed:]
		if i := bytes.IndexByte(rest, '\n'); i >= 0 {
			n, err := tw.w.Write(rest[:i+1])
			consumed += n
			if err != nil {
				return consumed, err
			}
			tw.atLineStart = true
		} else {
			n, err := tw.w.Write(rest)
			consumed += n
			if err != nil {
				return consumed, err
			}
		}
	}
	return consumed, nil
}
