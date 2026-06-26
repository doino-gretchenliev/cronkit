package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "jobs.yml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfigDefaults(t *testing.T) {
	c, err := LoadConfig(writeConfig(t, "jobs:\n  - name: a\n    schedule: \"@hourly\"\n    command: \"true\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.KeepRuns != 50 {
		t.Errorf("KeepRuns default = %d, want 50", c.KeepRuns)
	}
	if c.MaxLogBytes != defaultMaxLogBytes {
		t.Errorf("MaxLogBytes default = %d, want %d", c.MaxLogBytes, defaultMaxLogBytes)
	}
	if c.Timezone != "Local" {
		t.Errorf("Timezone default = %q", c.Timezone)
	}
}

func TestLoadConfigRejectsUnknownChain(t *testing.T) {
	_, err := LoadConfig(writeConfig(t, "jobs:\n  - name: a\n    schedule: \"@hourly\"\n    command: \"true\"\n    on_success: [nope]\n"))
	if err == nil {
		t.Fatal("expected error for chain to unknown job")
	}
}

func TestNotifyValidation(t *testing.T) {
	// missing from/to should fail
	if _, err := LoadConfig(writeConfig(t, "notify:\n  smtp_host: mail\njobs:\n  - name: a\n    command: \"true\"\n")); err == nil {
		t.Error("expected notify validation error")
	}
	// complete notify should default port + on
	c, err := LoadConfig(writeConfig(t, "notify:\n  smtp_host: mail\n  from: c@x\n  to: [me@x]\njobs:\n  - name: a\n    command: \"true\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Notify.SMTPPort != 25 {
		t.Errorf("default port = %d", c.Notify.SMTPPort)
	}
	if len(c.Notify.On) != 2 {
		t.Errorf("default on = %v", c.Notify.On)
	}
}

func TestKeepRunsForAndShouldNotify(t *testing.T) {
	c := &Config{KeepRuns: 50, Notify: &Notify{On: []RunStatus{StatusFailed}}}
	if got := c.keepRunsFor(Job{}); got != 50 {
		t.Errorf("keepRunsFor global = %d", got)
	}
	if got := c.keepRunsFor(Job{KeepRuns: 7}); got != 7 {
		t.Errorf("keepRunsFor override = %d", got)
	}
	if !c.shouldNotify(Job{}, StatusFailed) {
		t.Error("should notify on failed")
	}
	if c.shouldNotify(Job{}, StatusSuccess) {
		t.Error("should not notify on success")
	}
	off := false
	if c.shouldNotify(Job{Notify: &off}, StatusFailed) {
		t.Error("per-job notify:false should suppress")
	}
}
