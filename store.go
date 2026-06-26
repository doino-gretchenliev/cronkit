package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// RunStatus is the terminal (or in-progress) state of a single execution.
type RunStatus string

const (
	StatusRunning     RunStatus = "running"
	StatusSuccess     RunStatus = "success"
	StatusFailed      RunStatus = "failed"
	StatusTimeout     RunStatus = "timeout"
	StatusCanceled    RunStatus = "canceled"
	StatusInterrupted RunStatus = "interrupted" // process exited (e.g. restart) while running
)

// Run is the record of one execution of a job. It is persisted as meta.json
// next to the run's output.log under <data>/<job>/<id>/.
type Run struct {
	ID       string    `json:"id"`      // start time as unix-nanos string (sortable, unique)
	Job      string    `json:"job"`     //
	Trigger  string    `json:"trigger"` // "schedule" or "manual"
	Start    time.Time `json:"start"`
	End      time.Time `json:"end,omitempty"`
	ExitCode int       `json:"exit_code"`
	Status   RunStatus `json:"status"`
}

// Duration is the elapsed time; for a running job it's time since start.
func (r Run) Duration() time.Duration {
	if r.End.IsZero() {
		return time.Since(r.Start)
	}
	return r.End.Sub(r.Start)
}

// Store persists run records on disk. No database; one directory per run.
type Store struct {
	dir string
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) jobDir(job string) string         { return filepath.Join(s.dir, job) }
func (s *Store) runDir(job, id string) string     { return filepath.Join(s.dir, job, id) }
func (s *Store) metaPath(job, id string) string   { return filepath.Join(s.runDir(job, id), "meta.json") }
func (s *Store) LogPath(job, id string) string    { return filepath.Join(s.runDir(job, id), "output.log") }

// LogTail returns up to max trailing bytes of a run's log (whole-line aligned).
func (s *Store) LogTail(job, id string, max int64) string {
	f, err := os.Open(s.LogPath(job, id))
	if err != nil {
		return ""
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return ""
	}
	if fi.Size() > max {
		_, _ = f.Seek(-max, io.SeekEnd)
	}
	b, _ := io.ReadAll(f)
	if fi.Size() > max {
		if i := indexByte(b, '\n'); i >= 0 {
			b = b[i+1:]
		}
	}
	return string(b)
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

func (s *Store) writeMeta(r *Run) error {
	if err := os.MkdirAll(s.runDir(r.Job, r.ID), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.metaPath(r.Job, r.ID), b, 0o644)
}

// ReadRun loads a single run's metadata.
func (s *Store) ReadRun(job, id string) (*Run, error) {
	b, err := os.ReadFile(s.metaPath(job, id))
	if err != nil {
		return nil, err
	}
	var r Run
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// ListRuns returns a job's runs, newest first.
func (s *Store) ListRuns(job string) []*Run {
	entries, err := os.ReadDir(s.jobDir(job))
	if err != nil {
		return nil
	}
	runs := make([]*Run, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		r, err := s.ReadRun(job, e.Name())
		if err != nil {
			continue
		}
		runs = append(runs, r)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].Start.After(runs[j].Start) })
	return runs
}

// LastRun returns the most recent run, or nil.
func (s *Store) LastRun(job string) *Run {
	if runs := s.ListRuns(job); len(runs) > 0 {
		return runs[0]
	}
	return nil
}

// prune enforces retention: keep at most keepRuns newest runs, and (if keepDays
// > 0) drop runs older than keepDays. A run is removed if it fails either rule.
func (s *Store) prune(job string, keepRuns, keepDays int) {
	runs := s.ListRuns(job) // newest first
	var cutoff time.Time
	if keepDays > 0 {
		cutoff = time.Now().AddDate(0, 0, -keepDays)
	}
	for i, r := range runs {
		overCount := keepRuns > 0 && i >= keepRuns
		tooOld := keepDays > 0 && r.Start.Before(cutoff)
		if overCount || tooOld {
			_ = os.RemoveAll(s.runDir(job, r.ID))
		}
	}
}

// ReconcileRunning marks any run still in "running" state (left over from a
// crash/restart) as interrupted. Returns how many it fixed.
func (s *Store) ReconcileRunning() int {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0
	}
	fixed := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		for _, r := range s.ListRuns(e.Name()) {
			if r.Status != StatusRunning {
				continue
			}
			r.Status = StatusInterrupted
			if r.End.IsZero() {
				r.End = time.Now()
			}
			if r.ExitCode == 0 {
				r.ExitCode = -1
			}
			if s.writeMeta(r) == nil {
				fixed++
			}
		}
	}
	return fixed
}

// --- runtime state (jobs disabled from the UI) -------------------------------

func (s *Store) statePath() string { return filepath.Join(s.dir, "state.json") }

type persistedState struct {
	Disabled []string `json:"disabled"`
}

// LoadDisabled reads the set of UI-disabled job names (empty if none/absent).
func (s *Store) LoadDisabled() map[string]bool {
	set := map[string]bool{}
	b, err := os.ReadFile(s.statePath())
	if err != nil {
		return set
	}
	var st persistedState
	if json.Unmarshal(b, &st) == nil {
		for _, n := range st.Disabled {
			set[n] = true
		}
	}
	return set
}

// SaveDisabled persists the set of UI-disabled job names.
func (s *Store) SaveDisabled(set map[string]bool) error {
	names := make([]string, 0, len(set))
	for n, on := range set {
		if on {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	b, err := json.MarshalIndent(persistedState{Disabled: names}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.statePath(), b, 0o644)
}
