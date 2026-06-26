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
)

// maxChainDepth guards against chain cycles (A→B→A).
const maxChainDepth = 16

// Runner executes jobs, records each execution, enforces single-instance +
// group concurrency, and fires chained jobs on completion.
type Runner struct {
	cfgp  *atomic.Pointer[Config] // current config (swapped on reload)
	store *Store

	mu        sync.Mutex
	running   map[string]bool               // job name -> in flight (single-instance guard)
	groupBusy map[string]bool               // group -> a member is in flight
	cancels   map[string]context.CancelFunc // job name -> cancel its current run
	disabled  map[string]bool               // job name -> disabled from the UI
}

func NewRunner(cfgp *atomic.Pointer[Config], store *Store) *Runner {
	return &Runner{
		cfgp:      cfgp,
		store:     store,
		running:   map[string]bool{},
		groupBusy: map[string]bool{},
		cancels:   map[string]context.CancelFunc{},
		disabled:  store.LoadDisabled(),
	}
}

func (rn *Runner) cfg() *Config { return rn.cfgp.Load() }

// IsRunning reports whether the named job currently has an execution in flight.
func (rn *Runner) IsRunning(job string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.running[job]
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
	return rn.run(job, trigger, 0)
}

func (rn *Runner) run(job Job, trigger string, depth int) (*Run, error) {
	if rn.IsDisabled(job.Name) {
		return nil, ErrDisabled
	}
	if err := rn.acquire(job); err != nil {
		return nil, err
	}
	r, err := rn.execute(job, trigger)
	rn.release(job) // release before chaining so a same-group child can run
	if err != nil {
		return nil, err
	}
	rn.chain(job, r.Status, depth)
	return r, nil
}

// execute runs the command and persists the run record. It does not touch the
// concurrency locks (the caller owns them).
func (rn *Runner) execute(job Job, trigger string) (*Run, error) {
	start := time.Now()
	r := &Run{
		ID:      strconv.FormatInt(start.UnixNano(), 10),
		Job:     job.Name,
		Trigger: trigger,
		Start:   start,
		Status:  StatusRunning,
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
			if _, err := rn.run(child, "chain", depth+1); err != nil {
				log.Printf("chained job %s skipped: %v", name, err)
			}
		}
	}()
}
