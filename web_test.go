package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func newTestServer(t *testing.T, cfg *Config) *Server {
	t.Helper()
	if cfg.MaxLogBytes == 0 {
		cfg.MaxLogBytes = defaultMaxLogBytes
	}
	if cfg.KeepRuns == 0 {
		cfg.KeepRuns = 10
	}
	if cfg.Timezone == "" {
		cfg.Timezone = "UTC"
	}
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfgp := new(atomic.Pointer[Config])
	cfgp.Store(cfg)
	runner := NewRunner(cfgp, store)
	sched, err := NewScheduler(cfg, runner)
	if err != nil {
		t.Fatal(err)
	}
	schedp := new(atomic.Pointer[Scheduler])
	schedp.Store(sched)
	srv, err := NewServer(cfgp, schedp, store, runner, "jobs.yml", "test")
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func get(t *testing.T, srv *Server, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// Renders the main pages for jobs in assorted states; guards against
// template/data mismatches (e.g. a missing map key in a comparison).
func TestPagesRender(t *testing.T) {
	cfg := &Config{Jobs: []Job{
		{Name: "scheduled", Schedule: "@every 1h", Command: "true"},
		{Name: "debounced", Command: "true", Debounce: "5m"},
		{Name: "grouped", Group: "g", Command: "true"},
		{Name: "off", Schedule: "@daily", Command: "true", Enabled: func() *bool { b := false; return &b }()},
	}}
	srv := newTestServer(t, cfg)

	for _, p := range []string{"/", "/settings", "/metrics", "/healthz",
		"/job/scheduled", "/job/debounced", "/job/grouped", "/job/off"} {
		if code := get(t, srv, p); code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", p, code)
		}
	}
}
