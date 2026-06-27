package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Concurrency errors. A denied run is skipped (not queued).
var (
	// ErrBusy: this job already has an execution in flight (single-instance).
	ErrBusy = errors.New("job already running")
	// ErrGroupBusy: another job in this job's concurrency group is running.
	ErrGroupBusy = errors.New("group busy")
	// ErrDisabled: the job is disabled from the UI.
	ErrDisabled = errors.New("job disabled")
	// ErrDraining: drain mode is on — no new runs start.
	ErrDraining = errors.New("draining: new runs paused")
)

// maxChainDepth guards against chain cycles (A→B→A).
const maxChainDepth = 16

// Runner executes jobs, records each execution, enforces single-instance +
// group concurrency, and fires chained jobs on completion.
type Runner struct {
	cfgp     *atomic.Pointer[Config] // current config (swapped on reload)
	store    *Store
	draining atomic.Bool // drain mode: block new runs (running jobs finish)

	mu        sync.Mutex
	running   map[string]bool               // job name -> in flight (single-instance guard)
	groupBusy map[string]bool               // group -> a member is in flight
	cancels   map[string]context.CancelFunc // job name -> cancel its current run
	disabled  map[string]bool               // job name -> disabled from the UI
	pending   map[string]int                // job name -> coalesced triggers awaiting a run
	timers    map[string]*time.Timer        // job name -> debounce timer
	firstAt   map[string]time.Time          // job name -> first trigger of the current batch
	firesAt   map[string]time.Time          // job name -> when the armed debounce timer fires
}

func NewRunner(cfgp *atomic.Pointer[Config], store *Store) *Runner {
	return &Runner{
		cfgp:      cfgp,
		store:     store,
		running:   map[string]bool{},
		groupBusy: map[string]bool{},
		cancels:   map[string]context.CancelFunc{},
		disabled:  store.LoadDisabled(),
		pending:   map[string]int{},
		timers:    map[string]*time.Timer{},
		firstAt:   map[string]time.Time{},
		firesAt:   map[string]time.Time{},
	}
}

// Waiting reports a job's debounce/coalesce state for the UI/API.
type Waiting struct {
	Count   int       // triggers coalesced and awaiting a run
	Armed   bool      // a debounce timer is armed (waiting to fire)
	FiresAt time.Time // when it will fire (valid if Armed)
}

// GroupBusy reports whether a job in the given concurrency group is running.
func (rn *Runner) GroupBusy(group string) bool {
	if group == "" {
		return false
	}
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.groupBusy[group]
}

func (rn *Runner) Waiting(name string) Waiting {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	w := Waiting{Count: rn.pending[name]}
	if _, ok := rn.timers[name]; ok {
		w.Armed = true
		w.FiresAt = rn.firesAt[name]
	}
	return w
}

// TriggerResult describes what a Trigger call did.
type TriggerResult struct {
	Action  string    // "started" | "debounced" | "coalesced" | "disabled"
	FiresAt time.Time // when a debounced run will fire (Action == "debounced")
}

// ForceRun runs the job now in the background, bypassing the debounce window. It
// absorbs any pending batch (cancels the armed timer and folds its count into
// this run), since forcing a run is what those queued triggers wanted.
// Single-instance and group guards still apply.
func (rn *Runner) ForceRun(job Job) {
	if rn.draining.Load() {
		return
	}
	rn.mu.Lock()
	n := rn.pending[job.Name]
	delete(rn.pending, job.Name)
	if t := rn.timers[job.Name]; t != nil {
		t.Stop()
		delete(rn.timers, job.Name)
	}
	delete(rn.firstAt, job.Name)
	delete(rn.firesAt, job.Name)
	rn.mu.Unlock()
	go func() { _, _ = rn.run(job, "manual", 0, n) }()
}

// Trigger is the entry point for schedule/manual/api triggers. Jobs without
// debounce start a run immediately (skipped if already running, as before).
// Debounced jobs coalesce a burst into a single trailing run.
func (rn *Runner) Trigger(job Job, source string) TriggerResult {
	if rn.IsDisabled(job.Name) {
		return TriggerResult{Action: "disabled"}
	}
	if rn.draining.Load() {
		return TriggerResult{Action: "paused"}
	}
	deb := job.DebounceDur()
	if deb <= 0 {
		go func() { _, _ = rn.run(job, source, 0, 0) }()
		return TriggerResult{Action: "started"}
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.pending[job.Name]++
	if rn.running[job.Name] {
		return TriggerResult{Action: "coalesced"} // honored on finish (drainAfter)
	}
	now := time.Now()
	if rn.firstAt[job.Name].IsZero() {
		rn.firstAt[job.Name] = now
	}
	delay := deb
	if max := job.DebounceMaxDur(); max > 0 {
		if rem := rn.firstAt[job.Name].Add(max).Sub(now); rem < delay {
			delay = rem
		}
	}
	if delay < 0 {
		delay = 0
	}
	if t := rn.timers[job.Name]; t != nil {
		t.Stop()
	}
	name := job.Name
	rn.firesAt[name] = now.Add(delay)
	rn.timers[name] = time.AfterFunc(delay, func() { rn.fireBatch(name) })
	return TriggerResult{Action: "debounced", FiresAt: now.Add(delay)}
}

// fireBatch runs the coalesced batch once the debounce window goes quiet.
func (rn *Runner) fireBatch(name string) {
	rn.mu.Lock()
	delete(rn.timers, name)
	delete(rn.firstAt, name)
	delete(rn.firesAt, name)
	if rn.running[name] || rn.draining.Load() {
		rn.mu.Unlock() // pending stays; fired later (on finish, or when drain lifts via a new trigger)
		return
	}
	n := rn.pending[name]
	rn.pending[name] = 0
	rn.mu.Unlock()
	if n == 0 {
		return
	}
	if job, ok := rn.cfg().Job(name); ok {
		go func() { _, _ = rn.run(job, "debounced", 0, n) }()
	}
}

// drainAfter fires one pending coalesced run when a run finishes — for the same
// job (coalesce-while-running) or a same-group job that was waiting on the group.
func (rn *Runner) drainAfter(finished Job) {
	if rn.draining.Load() {
		return // don't start new runs while draining
	}
	rn.mu.Lock()
	candidates := []string{finished.Name}
	if finished.Group != "" {
		for _, j := range rn.cfg().Jobs {
			if j.Group == finished.Group && j.Name != finished.Name {
				candidates = append(candidates, j.Name)
			}
		}
	}
	var fireName string
	var fireN int
	for _, name := range candidates {
		if rn.pending[name] == 0 || rn.running[name] {
			continue
		}
		if finished.Group != "" && rn.groupBusy[finished.Group] {
			break // group re-acquired meanwhile; that run's finish will drain
		}
		fireName, fireN = name, rn.pending[name]
		rn.pending[name] = 0
		break
	}
	rn.mu.Unlock()
	if fireName == "" {
		return
	}
	if job, ok := rn.cfg().Job(fireName); ok {
		go func() { _, _ = rn.run(job, "debounced", 0, fireN) }()
	}
}

func (rn *Runner) cfg() *Config { return rn.cfgp.Load() }

// IsRunning reports whether the named job currently has an execution in flight.
func (rn *Runner) IsRunning(job string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.running[job]
}

// SetDraining toggles drain mode: when on, no new runs start (in-flight runs
// finish). Not persisted — a restart clears it.
func (rn *Runner) SetDraining(on bool) { rn.draining.Store(on) }

// Draining reports whether drain mode is on.
func (rn *Runner) Draining() bool { return rn.draining.Load() }

// RunningCount returns how many jobs are currently executing.
func (rn *Runner) RunningCount() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return len(rn.running)
}

// IsDisabled reports whether the job is disabled from the UI.
func (rn *Runner) IsDisabled(job string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.disabled[job]
}

// SetDisabled enables/disables a job and persists the change.
func (rn *Runner) SetDisabled(job string, off bool) {
	rn.mu.Lock()
	if off {
		rn.disabled[job] = true
	} else {
		delete(rn.disabled, job)
	}
	snapshot := make(map[string]bool, len(rn.disabled))
	for k, v := range rn.disabled {
		snapshot[k] = v
	}
	rn.mu.Unlock()
	_ = rn.store.SaveDisabled(snapshot)
}

// Cancel signals the job's in-flight run to stop. Returns false if not running.
func (rn *Runner) Cancel(job string) bool {
	rn.mu.Lock()
	c := rn.cancels[job]
	rn.mu.Unlock()
	if c == nil {
		return false
	}
	c()
	return true
}

func (rn *Runner) setCancel(job string, c context.CancelFunc) {
	rn.mu.Lock()
	rn.cancels[job] = c
	rn.mu.Unlock()
}

func (rn *Runner) clearCancel(job string) {
	rn.mu.Lock()
	delete(rn.cancels, job)
	rn.mu.Unlock()
}

// acquire reserves the job (and its group). Returns ErrBusy / ErrGroupBusy if
// either is already taken.
func (rn *Runner) acquire(job Job) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.running[job.Name] {
		return ErrBusy
	}
	if job.Group != "" && rn.groupBusy[job.Group] {
		return ErrGroupBusy
	}
	rn.running[job.Name] = true
	if job.Group != "" {
		rn.groupBusy[job.Group] = true
	}
	return nil
}

func (rn *Runner) release(job Job) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	delete(rn.running, job.Name)
	if job.Group != "" {
		delete(rn.groupBusy, job.Group)
	}
}

// Run executes the job synchronously, then triggers any chained jobs.
// trigger is "schedule", "manual" or "chain".
func (rn *Runner) Run(job Job, trigger string) (*Run, error) {
	return rn.run(job, trigger, 0, 0)
}

func (rn *Runner) run(job Job, trigger string, depth, coalesced int) (*Run, error) {
	if rn.IsDisabled(job.Name) {
		return nil, ErrDisabled
	}
	if rn.draining.Load() {
		return nil, ErrDraining
	}
	if err := rn.acquire(job); err != nil {
		// Debounced jobs coalesce a trigger that lands while busy into one
		// trailing run, fired when the job/group frees up (drainAfter).
		if job.DebounceDur() > 0 && (errors.Is(err, ErrBusy) || errors.Is(err, ErrGroupBusy)) {
			rn.mu.Lock()
			rn.pending[job.Name]++
			rn.mu.Unlock()
		}
		return nil, err
	}
	r, err := rn.execute(job, trigger, coalesced)
	rn.release(job) // release before chaining so a same-group child can run
	// Always drain: even a non-debounced job that just freed a group may have a
	// debounced group member waiting on it. No-op when nothing is pending.
	rn.drainAfter(job)
	if err != nil {
		return nil, err
	}
	rn.chain(job, r.Status, depth)
	return r, nil
}

// execute runs the command and persists the run record. It does not touch the
// concurrency locks (the caller owns them).
func (rn *Runner) execute(job Job, trigger string, coalesced int) (*Run, error) {
	start := time.Now()
	r := &Run{
		ID:        strconv.FormatInt(start.UnixNano(), 10),
		Job:       job.Name,
		Trigger:   trigger,
		Coalesced: coalesced,
		Start:     start,
		Status:    StatusRunning,
	}
	if err := rn.store.writeMeta(r); err != nil {
		return nil, err
	}

	logf, err := os.Create(rn.store.LogPath(job.Name, r.ID))
	if err != nil {
		return nil, err
	}
	defer logf.Close()

	// One cancelable context drives both the manual Cancel and the timeout.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rn.setCancel(job.Name, cancel)
	defer rn.clearCancel(job.Name)

	var timedOut atomic.Bool
	if d := job.TimeoutDur(); d > 0 {
		t := time.AfterFunc(d, func() { timedOut.Store(true); cancel() })
		defer t.Stop()
	}

	cfg := rn.cfg()
	loc := cfg.Location()
	tw := newTimestampWriter(newCapWriter(logf, cfg.MaxLogBytes), loc)
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", job.Command)
	cmd.Stdout = tw
	cmd.Stderr = tw
	cmd.Env = os.Environ()
	// Run in its own process group so a timeout/cancel kills the whole tree, not
	// just the shell — otherwise a grandchild (e.g. `sleep`) keeps the output
	// pipe open and Run() blocks until it exits, defeating the timeout.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// Negative pid = the whole process group (see Setpgid above).
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	// Backstop: if I/O is still pending shortly after the process is signaled
	// (a daemonized grandchild holding the pipe), force the pipes closed.
	cmd.WaitDelay = 5 * time.Second

	runErr := cmd.Run()

	r.End = time.Now()
	if cmd.ProcessState != nil {
		r.ExitCode = cmd.ProcessState.ExitCode()
	} else {
		r.ExitCode = -1
	}
	switch {
	case timedOut.Load():
		r.Status = StatusTimeout
	case ctx.Err() == context.Canceled:
		r.Status = StatusCanceled
	case runErr == nil:
		r.Status = StatusSuccess
	default:
		r.Status = StatusFailed
	}

	_ = logf.Sync()
	_ = rn.store.writeMeta(r)
	rn.store.prune(job.Name, cfg.keepRunsFor(job), job.KeepDays)

	if cfg.shouldNotify(job, r.Status) {
		tail := rn.store.LogTail(job.Name, r.ID, 4096)
		run := *r
		go func() {
			if err := sendFailureEmail(cfg.Notify, job, &run, loc, tail); err != nil {
				log.Printf("notify %s: %v", job.Name, err)
			}
		}()
	}
	return r, nil
}

// chain fires downstream jobs after a run finishes, in listed order, in a
// background goroutine. Each child runs under its own single-instance + group
// guards, so a busy child is skipped.
func (rn *Runner) chain(job Job, status RunStatus, depth int) {
	var next []string
	switch status {
	case StatusSuccess:
		next = job.OnSuccess
	case StatusFailed, StatusTimeout:
		next = job.OnFailure
	}
	if len(next) == 0 {
		return
	}
	if depth+1 > maxChainDepth {
		log.Printf("job %s: chain depth limit (%d) reached; not triggering %v", job.Name, maxChainDepth, next)
		return
	}
	cfg := rn.cfg()
	go func() {
		for _, name := range next {
			child, ok := cfg.Job(name)
			if !ok { // shouldn't happen — validated at load
				log.Printf("job %s: chained job %q not found", job.Name, name)
				continue
			}
			if _, err := rn.run(child, "chain", depth+1, 0); err != nil {
				log.Printf("chained job %s skipped: %v", name, err)
			}
		}
	}()
}
