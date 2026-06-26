package main

import (
	"sync/atomic"
	"testing"
	"time"
)

func newTestRunner(t *testing.T, cfg *Config) *Runner {
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
	st, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfgp := new(atomic.Pointer[Config])
	cfgp.Store(cfg)
	return NewRunner(cfgp, st)
}

// waitRunning polls until the job reports running (or fails the test).
func waitRunning(t *testing.T, rn *Runner, job string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if rn.IsRunning(job) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s never started", job)
}

func TestRunSuccessAndFail(t *testing.T) {
	rn := newTestRunner(t, &Config{})
	r, err := rn.Run(Job{Name: "ok", Command: "echo hi"}, "manual")
	if err != nil || r.Status != StatusSuccess || r.ExitCode != 0 {
		t.Fatalf("success run: %+v err=%v", r, err)
	}
	r, err = rn.Run(Job{Name: "bad", Command: "exit 3"}, "manual")
	if err != nil || r.Status != StatusFailed || r.ExitCode != 3 {
		t.Fatalf("fail run: %+v err=%v", r, err)
	}
}

func TestRunTimeout(t *testing.T) {
	rn := newTestRunner(t, &Config{})
	start := time.Now()
	r, err := rn.Run(Job{Name: "slow", Command: "sleep 5", Timeout: "500ms"}, "manual")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != StatusTimeout {
		t.Errorf("status = %s, want timeout", r.Status)
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("timeout took %v, expected ~0.5s", time.Since(start))
	}
}

func TestDisabledBlocksRun(t *testing.T) {
	rn := newTestRunner(t, &Config{})
	rn.SetDisabled("j", true)
	if _, err := rn.Run(Job{Name: "j", Command: "true"}, "manual"); err != ErrDisabled {
		t.Errorf("got %v, want ErrDisabled", err)
	}
	rn.SetDisabled("j", false)
	if _, err := rn.Run(Job{Name: "j", Command: "true"}, "manual"); err != nil {
		t.Errorf("re-enabled run failed: %v", err)
	}
}

func TestSingleInstance(t *testing.T) {
	rn := newTestRunner(t, &Config{})
	job := Job{Name: "x", Command: "sleep 1"}
	go rn.Run(job, "manual")
	waitRunning(t, rn, "x")
	if _, err := rn.Run(job, "manual"); err != ErrBusy {
		t.Errorf("got %v, want ErrBusy", err)
	}
}

func TestGroupExclusion(t *testing.T) {
	rn := newTestRunner(t, &Config{})
	go rn.Run(Job{Name: "a", Group: "g", Command: "sleep 1"}, "manual")
	waitRunning(t, rn, "a")
	if _, err := rn.Run(Job{Name: "b", Group: "g", Command: "true"}, "manual"); err != ErrGroupBusy {
		t.Errorf("got %v, want ErrGroupBusy", err)
	}
}

func TestCancel(t *testing.T) {
	rn := newTestRunner(t, &Config{})
	done := make(chan *Run, 1)
	go func() {
		r, _ := rn.Run(Job{Name: "c", Command: "sleep 5"}, "manual")
		done <- r
	}()
	waitRunning(t, rn, "c")
	if !rn.Cancel("c") {
		t.Fatal("Cancel returned false")
	}
	select {
	case r := <-done:
		if r.Status != StatusCanceled {
			t.Errorf("status = %s, want canceled", r.Status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not stop the job")
	}
}

func TestChaining(t *testing.T) {
	cfg := &Config{Jobs: []Job{
		{Name: "a", Command: "true", OnSuccess: []string{"b"}},
		{Name: "b", Command: "true"},
	}}
	rn := newTestRunner(t, cfg)
	a, _ := cfg.Job("a")
	if _, err := rn.Run(a, "manual"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if rn.store.LastRun("b") != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("chained job b never ran")
}
