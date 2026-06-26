package main

import (
	"testing"
	"time"
)

func TestHumanDur(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "0ms"},
		{250 * time.Millisecond, "250ms"},
		{1500 * time.Millisecond, "1.5s"},
		{45 * time.Second, "45s"},
		{3*time.Minute + 20*time.Second, "3m 20s"},
		{90 * time.Minute, "1h 30m"},
		{26 * time.Hour, "1d 2h"},
	}
	for _, c := range cases {
		if got := humanDur(c.d); got != c.want {
			t.Errorf("humanDur(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestRelWords(t *testing.T) {
	now := time.Now()
	// add a half-second buffer so integer truncation lands in the right bucket
	cases := []struct {
		t    time.Time
		want string
	}{
		{now.Add(-5*time.Minute - 500*time.Millisecond), "5 mins ago"},
		{now.Add(-1*time.Minute - 500*time.Millisecond), "1 min ago"},
		{now.Add(2*time.Hour + 500*time.Millisecond), "in 2 hrs"},
		{now.Add(30*time.Second + 500*time.Millisecond), "in 30 secs"},
	}
	for _, c := range cases {
		if got := relWords(c.t); got != c.want {
			t.Errorf("relWords(%v) = %q, want %q", c.t, got, c.want)
		}
	}
}

func TestStatusClass(t *testing.T) {
	for st, want := range map[RunStatus]string{
		StatusSuccess:     "ok",
		StatusFailed:      "fail",
		StatusTimeout:     "warn",
		StatusCanceled:    "canceled",
		StatusInterrupted: "interrupted",
		StatusRunning:     "running",
	} {
		if got := statusClass(st); got != want {
			t.Errorf("statusClass(%s) = %q, want %q", st, got, want)
		}
	}
}
