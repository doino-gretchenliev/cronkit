package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Job is a single command. It may be scheduled, triggered manually, and/or
// triggered by another job's completion (chaining).
type Job struct {
	Name     string `yaml:"name"`
	Schedule string `yaml:"schedule"` // cron expr or @-descriptor; empty = manual/chain only
	Command  string `yaml:"command"`  // run via `/bin/sh -c`
	Enabled  *bool  `yaml:"enabled"`  // default true
	Timeout  string `yaml:"timeout"`  // optional Go duration, e.g. "10m"; empty = no timeout

	// Group is a concurrency group: at most one job in a group runs at a time.
	// A run requested while the group is busy is denied (skipped), not queued.
	Group string `yaml:"group"`

	// Chaining: job names to trigger after this job finishes. on_success fires
	// only when this job succeeds; on_failure when it fails or times out. Chained
	// jobs run in listed order and may themselves chain (cycle-guarded by depth).
	OnSuccess []string `yaml:"on_success"`
	OnFailure []string `yaml:"on_failure"`

	// Description is a free-text note shown in the UI.
	Description string `yaml:"description"`

	// Notify, if false, suppresses failure emails for this job (default: notify
	// per the global notify config).
	Notify *bool `yaml:"notify"`

	// Per-job history retention overrides (fall back to the global keep_runs).
	// keep_runs: keep at most N newest runs. keep_days: drop runs older than N
	// days. Both may be combined.
	KeepRuns int `yaml:"keep_runs"`
	KeepDays int `yaml:"keep_days"`

	// Debounce coalesces a burst of triggers into a single trailing run: a
	// trigger arms a timer for `debounce`; more triggers within the window reset
	// it; the job runs once when it goes quiet. `debounce_max` caps the total
	// delay from the first trigger so a continuous trickle still runs. Debounced
	// jobs also coalesce a trigger that lands while the job is running into one
	// trailing run on finish (so nothing is missed). Empty = run immediately.
	Debounce    string `yaml:"debounce"`
	DebounceMax string `yaml:"debounce_max"`
}

func parseDurOr0(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

func (j Job) DebounceDur() time.Duration    { return parseDurOr0(j.Debounce) }
func (j Job) DebounceMaxDur() time.Duration { return parseDurOr0(j.DebounceMax) }

// IsEnabled reports whether the job should be scheduled. Absent = enabled.
func (j Job) IsEnabled() bool { return j.Enabled == nil || *j.Enabled }

// TimeoutDur parses Timeout; returns 0 (no timeout) if empty or invalid.
func (j Job) TimeoutDur() time.Duration {
	if j.Timeout == "" {
		return 0
	}
	d, err := time.ParseDuration(j.Timeout)
	if err != nil {
		return 0
	}
	return d
}

// Notify configures failure emails sent through an SMTP relay.
type Notify struct {
	SMTPHost string      `yaml:"smtp_host"`
	SMTPPort int         `yaml:"smtp_port"` // default 25
	From     string      `yaml:"from"`
	To       []string    `yaml:"to"`
	On       []RunStatus `yaml:"on"` // statuses that trigger a mail; default failed,timeout
}

// Config is the whole jobs file.
type Config struct {
	Timezone    string  `yaml:"timezone"`      // IANA name, e.g. "Europe/Sofia"; default Local
	KeepRuns    int     `yaml:"keep_runs"`     // global history cap (per-job overridable)
	MaxLogBytes int64   `yaml:"max_log_bytes"` // per-run output.log cap; default 20MiB; <=0 => default
	Notify      *Notify `yaml:"notify"`        // optional failure email
	Jobs        []Job   `yaml:"jobs"`
}

const defaultMaxLogBytes = 20 << 20 // 20 MiB

// LoadConfig reads and validates the jobs file.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if c.KeepRuns <= 0 {
		c.KeepRuns = 50
	}
	if c.MaxLogBytes <= 0 {
		c.MaxLogBytes = defaultMaxLogBytes
	}
	if c.Timezone == "" {
		c.Timezone = "Local"
	}
	if _, err := time.LoadLocation(c.Timezone); err != nil {
		return nil, fmt.Errorf("unknown timezone %q: %w", c.Timezone, err)
	}
	if c.Notify != nil {
		n := c.Notify
		if n.SMTPHost == "" || n.From == "" || len(n.To) == 0 {
			return nil, fmt.Errorf("notify requires smtp_host, from and at least one to")
		}
		if n.SMTPPort == 0 {
			n.SMTPPort = 25
		}
		if len(n.On) == 0 {
			n.On = []RunStatus{StatusFailed, StatusTimeout}
		}
		for _, st := range n.On {
			switch st {
			case StatusSuccess, StatusFailed, StatusTimeout, StatusCanceled:
			default:
				return nil, fmt.Errorf("notify.on has unknown status %q", st)
			}
		}
	}
	seen := map[string]bool{}
	for i := range c.Jobs {
		j := c.Jobs[i]
		switch {
		case j.Name == "":
			return nil, fmt.Errorf("job #%d has no name", i+1)
		case seen[j.Name]:
			return nil, fmt.Errorf("duplicate job name %q", j.Name)
		case j.Command == "":
			return nil, fmt.Errorf("job %q has no command", j.Name)
		}
		// A job with no schedule is valid — it runs only via chaining or the
		// "run now" button.
		if _, err := j.parseTimeout(); err != nil {
			return nil, fmt.Errorf("job %q timeout %q: %w", j.Name, j.Timeout, err)
		}
		for label, val := range map[string]string{"debounce": j.Debounce, "debounce_max": j.DebounceMax} {
			if val != "" {
				if _, err := time.ParseDuration(val); err != nil {
					return nil, fmt.Errorf("job %q %s %q: %w", j.Name, label, val, err)
				}
			}
		}
		seen[j.Name] = true
	}
	// Validate chain references now that all names are known.
	for _, j := range c.Jobs {
		for _, ref := range append(append([]string{}, j.OnSuccess...), j.OnFailure...) {
			if !seen[ref] {
				return nil, fmt.Errorf("job %q chains to unknown job %q", j.Name, ref)
			}
		}
	}
	return &c, nil
}

func (j Job) parseTimeout() (time.Duration, error) {
	if j.Timeout == "" {
		return 0, nil
	}
	return time.ParseDuration(j.Timeout)
}

// Location resolves the configured timezone, falling back to Local.
func (c *Config) Location() *time.Location {
	loc, err := time.LoadLocation(c.Timezone)
	if err != nil {
		return time.Local
	}
	return loc
}

// Job looks up a job by name.
func (c *Config) Job(name string) (Job, bool) {
	for _, j := range c.Jobs {
		if j.Name == name {
			return j, true
		}
	}
	return Job{}, false
}

// keepRunsFor returns the effective run-count retention for a job.
func (c *Config) keepRunsFor(job Job) int {
	if job.KeepRuns > 0 {
		return job.KeepRuns
	}
	return c.KeepRuns
}

// shouldNotify reports whether a finished run should trigger a failure email.
func (c *Config) shouldNotify(job Job, st RunStatus) bool {
	if c.Notify == nil {
		return false
	}
	if job.Notify != nil && !*job.Notify {
		return false
	}
	for _, s := range c.Notify.On {
		if s == st {
			return true
		}
	}
	return false
}
