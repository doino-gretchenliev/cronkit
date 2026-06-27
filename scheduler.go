package main

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Scheduler wires enabled jobs into a robfig/cron instance. Scheduling lives
// in-process; like cron, a missed tick (e.g. while the host was down) is simply
// skipped rather than caught up.
type Scheduler struct {
	cron    *cron.Cron
	entries map[string]cron.EntryID // job name -> entry
}

func NewScheduler(cfg *Config, runner *Runner) (*Scheduler, error) {
	c := cron.New(cron.WithLocation(cfg.Location()))
	s := &Scheduler{cron: c, entries: map[string]cron.EntryID{}}

	for i := range cfg.Jobs {
		job := cfg.Jobs[i] // capture per iteration
		// Skip disabled jobs and jobs with no schedule (manual/chain-only).
		if !job.IsEnabled() || job.Schedule == "" {
			continue
		}
		id, err := c.AddFunc(job.Schedule, func() {
			runner.Trigger(job, "schedule")
		})
		if err != nil {
			return nil, fmt.Errorf("job %q schedule %q: %w", job.Name, job.Schedule, err)
		}
		s.entries[job.Name] = id
	}
	return s, nil
}

func (s *Scheduler) Start() { s.cron.Start() }
func (s *Scheduler) Stop()  { s.cron.Stop() }

// NextRun returns the next scheduled time for a job, if it is scheduled.
func (s *Scheduler) NextRun(job string) (time.Time, bool) {
	id, ok := s.entries[job]
	if !ok {
		return time.Time{}, false
	}
	e := s.cron.Entry(id)
	if e.ID == 0 || e.Next.IsZero() {
		return time.Time{}, false
	}
	return e.Next, true
}
