package main

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

//go:embed ui/*
var uiFS embed.FS

// displayLogCap bounds how much of a finished run's log we inline into the page.
// The full log is always available at .../raw.
const displayLogCap = 256 * 1024

// Server holds the HTTP handlers and rendered templates. The config and
// scheduler are behind atomic pointers so they can be swapped on reload.
type Server struct {
	cfgp       *atomic.Pointer[Config]
	schedp     *atomic.Pointer[Scheduler]
	store      *Store
	runner     *Runner
	configPath string
	version    string
	apikey     atomic.Pointer[string]
	tmpl       map[string]*template.Template
}

func genAPIKey() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return "ck_" + base64.RawURLEncoding.EncodeToString(b)
}

// APIKey returns the current integration API key.
func (s *Server) APIKey() string {
	if p := s.apikey.Load(); p != nil {
		return *p
	}
	return ""
}

// RotateAPIKey generates, persists, and installs a new API key.
func (s *Server) RotateAPIKey() string {
	k := genAPIKey()
	_ = s.store.SaveAPIKey(k)
	s.apikey.Store(&k)
	return k
}

func (s *Server) cfg() *Config        { return s.cfgp.Load() }
func (s *Server) sched() *Scheduler   { return s.schedp.Load() }
func (s *Server) loc() *time.Location { return s.cfg().Location() }

// Reload re-reads the config file and rebuilds the scheduler in place. On any
// error the running config is left untouched.
func (s *Server) Reload() error {
	c, err := LoadConfig(s.configPath)
	if err != nil {
		log.Printf("reload: %v", err)
		return err
	}
	ns, err := NewScheduler(c, s.runner)
	if err != nil {
		log.Printf("reload: %v", err)
		return err
	}
	s.cfgp.Store(c)
	ns.Start()
	if old := s.schedp.Swap(ns); old != nil {
		old.Stop()
	}
	log.Printf("config reloaded: %d jobs", len(c.Jobs))
	return nil
}

func NewServer(cfgp *atomic.Pointer[Config], schedp *atomic.Pointer[Scheduler], store *Store, runner *Runner, configPath, version string) (*Server, error) {
	s := &Server{cfgp: cfgp, schedp: schedp, store: store, runner: runner, configPath: configPath, version: version}

	key := store.LoadAPIKey()
	if key == "" {
		key = genAPIKey()
		_ = store.SaveAPIKey(key)
	}
	s.apikey.Store(&key)

	funcs := template.FuncMap{
		"dur":         humanDur,
		"fmttime":     s.fmttime,
		"reltime":     s.reltime,
		"statusClass": statusClass,
		"sparkline":   sparkline,
		"version":     func() string { return s.version },
	}
	s.tmpl = map[string]*template.Template{}
	for _, page := range []string{"index.html", "job.html", "run.html", "settings.html"} {
		t, err := template.New("layout.html").Funcs(funcs).ParseFS(uiFS, "ui/layout.html", "ui/"+page)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", page, err)
		}
		s.tmpl[page] = t
	}
	return s, nil
}

func (s *Server) Mux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /job/{name}", s.handleJob)
	mux.HandleFunc("GET /job/{name}/run/{id}", s.handleRun)
	mux.HandleFunc("GET /job/{name}/run/{id}/raw", s.handleRaw)
	mux.HandleFunc("GET /job/{name}/run/{id}/stream", s.handleStream)
	mux.HandleFunc("POST /job/{name}/run", s.handleRunNow)
	mux.HandleFunc("POST /job/{name}/cancel", s.handleCancel)
	mux.HandleFunc("POST /job/{name}/toggle", s.handleToggle)
	mux.HandleFunc("POST /reload", s.handleReload)
	mux.HandleFunc("POST /drain", s.handleDrain(true))
	mux.HandleFunc("POST /resume", s.handleDrain(false))
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	// Settings page (open, like the rest of the UI) — shows + rotates the API key.
	mux.HandleFunc("GET /settings", s.handleSettings)
	mux.HandleFunc("POST /settings/rotate-key", s.handleRotateKey)

	// Integration API — protected by the single API key.
	mux.HandleFunc("GET /api/jobs", s.withAPIKey(s.apiListJobs))
	mux.HandleFunc("POST /api/jobs/{name}/run", s.withAPIKey(s.apiRun))
	mux.HandleFunc("POST /api/jobs/{name}/cancel", s.withAPIKey(s.apiCancel))
	mux.HandleFunc("POST /api/jobs/{name}/disable", s.withAPIKey(s.apiSetDisabled(true)))
	mux.HandleFunc("POST /api/jobs/{name}/enable", s.withAPIKey(s.apiSetDisabled(false)))
	mux.HandleFunc("POST /api/drain", s.withAPIKey(s.apiDrain(true)))
	mux.HandleFunc("POST /api/resume", s.withAPIKey(s.apiDrain(false)))
	mux.HandleFunc("GET /static/style.css", s.handleCSS)
	mux.HandleFunc("GET /favicon.svg", s.handleFavicon)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok\n")
	})
	return logRequests(mux)
}

func (s *Server) render(w http.ResponseWriter, page string, data any) {
	t, ok := s.tmpl[page]
	if !ok {
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "layout.html", data); err != nil {
		log.Printf("render %s: %v", page, err)
		http.Error(w, "render error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

type jobRow struct {
	Job      Job
	Last     *Run
	RunCount int // recorded (retained) runs
	Next     *time.Time
	Running  bool
	Disabled bool       // disabled from the UI
	Enabled  bool       // effective: config-enabled AND not UI-disabled
	Pending  int           // coalesced triggers awaiting a run
	Fires    *time.Time    // when a debounced run will fire (if waiting)
	Blocked  bool          // waiting because a same-group job is running
	Bar      template.HTML // mini progress bar while running (elapsed vs avg)
}

// noGroup is the group-filter sentinel for "jobs with no group".
const noGroup = "__none__"

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	qLower := strings.ToLower(q)
	group := r.URL.Query().Get("group")

	// Distinct groups for the dropdown.
	set := map[string]bool{}
	for _, j := range s.cfg().Jobs {
		if j.Group != "" {
			set[j.Group] = true
		}
	}
	groups := make([]string, 0, len(set))
	for g := range set {
		groups = append(groups, g)
	}
	sort.Strings(groups)

	cfg := s.cfg()
	rows := make([]jobRow, 0, len(cfg.Jobs))
	for _, j := range cfg.Jobs {
		if qLower != "" && !strings.Contains(strings.ToLower(j.Name), qLower) {
			continue
		}
		switch {
		case group == noGroup && j.Group != "":
			continue
		case group != "" && group != noGroup && j.Group != group:
			continue
		}
		dis := s.runner.IsDisabled(j.Name)
		runs := s.store.ListRuns(j.Name)
		var last *Run
		if len(runs) > 0 {
			last = runs[0]
		}
		row := jobRow{
			Job:      j,
			Last:     last,
			RunCount: len(runs),
			Running:  s.runner.IsRunning(j.Name),
			Disabled: dis,
			Enabled:  j.IsEnabled() && !dis,
		}
		if n, ok := s.sched().NextRun(j.Name); ok {
			row.Next = &n
		}
		if w := s.runner.Waiting(j.Name); w.Count > 0 {
			row.Pending = w.Count
			if w.Armed {
				ft := w.FiresAt
				row.Fires = &ft
				if row.Next == nil {
					row.Next = &ft // surface the fire time for schedule-less jobs
				}
			}
			if !row.Running && s.runner.GroupBusy(j.Group) {
				row.Blocked = true
			}
		}
		if row.Running && last != nil {
			if avg := avgRunDuration(runs, 20); avg > 0 {
				pct := int(time.Since(last.Start) * 100 / avg)
				over := pct > 100
				if pct > 100 {
					pct = 100
				}
				if pct < 0 {
					pct = 0
				}
				row.Bar = progressBar(pct, over, avg)
			}
		}
		rows = append(rows, row)
	}

	s.render(w, "index.html", map[string]any{
		"Jobs":         rows,
		"Groups":       groups,
		"Q":            q,
		"Group":        group,
		"NoGroup":      noGroup,
		"Total":        len(cfg.Jobs),
		"Shown":        len(rows),
		"Filtered":     q != "" || group != "",
		"Draining":     s.runner.Draining(),
		"RunningCount": s.runner.RunningCount(),
	})
}

const runsPerPage = 25

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cfg := s.cfg()
	job, ok := cfg.Job(name)
	if !ok {
		http.NotFound(w, r)
		return
	}

	qs := r.URL.Query()
	statusF := qs.Get("status")
	exitF := strings.TrimSpace(qs.Get("exit"))
	dminF := strings.TrimSpace(qs.Get("dmin"))
	dmaxF := strings.TrimSpace(qs.Get("dmax"))
	sinceF := qs.Get("since")
	sortKey := qs.Get("sort")
	if sortKey == "" {
		sortKey = "started"
	}
	dir := qs.Get("dir")
	if dir != "asc" {
		dir = "desc"
	}
	page, _ := strconv.Atoi(qs.Get("page"))
	if page < 1 {
		page = 1
	}

	all := s.store.ListRuns(name) // newest first

	// Parse filter values.
	exitVal, hasExit := 0, false
	if v, err := strconv.Atoi(exitF); err == nil {
		exitVal, hasExit = v, true
	}
	dmin, hasDmin := parseSeconds(dminF)
	dmax, hasDmax := parseSeconds(dmaxF)
	sinceCut, hasSince := sinceCutoff(sinceF)

	filtered := make([]*Run, 0, len(all))
	for _, rn := range all {
		if statusF != "" && string(rn.Status) != statusF {
			continue
		}
		if hasExit && rn.ExitCode != exitVal {
			continue
		}
		d := rn.Duration().Seconds()
		if hasDmin && d < dmin {
			continue
		}
		if hasDmax && d > dmax {
			continue
		}
		if hasSince && rn.Start.Before(sinceCut) {
			continue
		}
		filtered = append(filtered, rn)
	}

	// Sort (SliceStable keeps the newest-first base order for equal keys).
	sort.SliceStable(filtered, func(i, j int) bool {
		a, b := filtered[i], filtered[j]
		var less bool
		switch sortKey {
		case "status":
			less = a.Status < b.Status
		case "duration":
			less = a.Duration() < b.Duration()
		case "exit":
			less = a.ExitCode < b.ExitCode
		default: // started
			less = a.Start.Before(b.Start)
		}
		if dir == "desc" {
			return !less
		}
		return less
	})

	// Paginate.
	total := len(filtered)
	pages := (total + runsPerPage - 1) / runsPerPage
	if pages < 1 {
		pages = 1
	}
	if page > pages {
		page = pages
	}
	start := (page - 1) * runsPerPage
	end := start + runsPerPage
	if end > total {
		end = total
	}
	var pageRuns []*Run
	if start < total {
		pageRuns = filtered[start:end]
	}

	// URL builder that preserves the current params, applying overrides.
	base := url.Values{}
	for _, k := range []string{"status", "exit", "dmin", "dmax", "since", "sort", "dir", "page"} {
		if v := qs.Get(k); v != "" {
			base.Set(k, v)
		}
	}
	base.Set("sort", sortKey)
	base.Set("dir", dir)
	withParams := func(over map[string]string) string {
		v := url.Values{}
		for k, vals := range base {
			v[k] = append([]string{}, vals...)
		}
		for k, val := range over {
			if val == "" {
				v.Del(k)
			} else {
				v.Set(k, val)
			}
		}
		return "/job/" + name + "?" + v.Encode()
	}

	// Sortable column headers with toggle direction + active arrow.
	type hdr struct{ Label, Href, Arrow string }
	headers := make([]hdr, 0, 4)
	for _, c := range []struct{ Key, Label string }{
		{"started", "Started"}, {"status", "Status"}, {"duration", "Duration"}, {"exit", "Exit"},
	} {
		nextDir, arrow := "desc", ""
		if sortKey == c.Key {
			if dir == "desc" {
				nextDir, arrow = "asc", "▼"
			} else {
				nextDir, arrow = "desc", "▲"
			}
		}
		headers = append(headers, hdr{c.Label, withParams(map[string]string{"sort": c.Key, "dir": nextDir, "page": "1"}), arrow})
	}

	prevHref, nextHref := "", ""
	if page > 1 {
		prevHref = withParams(map[string]string{"page": strconv.Itoa(page - 1)})
	}
	if page < pages {
		nextHref = withParams(map[string]string{"page": strconv.Itoa(page + 1)})
	}

	disabled := s.runner.IsDisabled(name)
	data := map[string]any{
		"Job":           job,
		"Running":       s.runner.IsRunning(name),
		"Disabled":      disabled,
		"Enabled":       job.IsEnabled() && !disabled,
		"Chain":         chainDiagram(cfg, job),
		"SparkRuns":     all,
		"Runs":          pageRuns,
		"Headers":       headers,
		"Status":        statusF,
		"Exit":          exitF,
		"Dmin":          dminF,
		"Dmax":          dmaxF,
		"Since":         sinceF,
		"Sort":          sortKey,
		"Dir":           dir,
		"Page":          page,
		"Pages":         pages,
		"Total":         total,
		"PrevHref":      prevHref,
		"NextHref":      nextHref,
		"FiltersActive": statusF != "" || exitF != "" || dminF != "" || dmaxF != "" || sinceF != "",
	}
	if n, ok := s.sched().NextRun(name); ok {
		data["Next"] = &n
	}
	wait := s.runner.Waiting(name)
	data["Pending"] = wait.Count // always set: the template does `gt .Pending 0`
	if wait.Armed {
		ft := wait.FiresAt
		data["Fires"] = &ft
	}
	if wait.Count > 0 && !s.runner.IsRunning(name) && s.runner.GroupBusy(job.Group) {
		data["Blocked"] = true
	}
	s.render(w, "job.html", data)
}

func parseSeconds(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func sinceCutoff(s string) (time.Time, bool) {
	var d time.Duration
	switch s {
	case "1m":
		d = time.Minute
	case "5m":
		d = 5 * time.Minute
	case "10m":
		d = 10 * time.Minute
	case "30m":
		d = 30 * time.Minute
	case "1h":
		d = time.Hour
	case "24h":
		d = 24 * time.Hour
	case "7d":
		d = 7 * 24 * time.Hour
	default:
		return time.Time{}, false
	}
	return time.Now().Add(-d), true
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	name, id := r.PathValue("name"), r.PathValue("id")
	run, err := s.store.ReadRun(name, id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	// Inline the last chunk of the log immediately (for both running and finished
	// runs). For a running run, the SSE stream then follows from the current end
	// of file so the browser isn't flooded with the whole (possibly huge) log.
	logText := s.readLogTail(name, id, displayLogCap)
	var offset int64
	if run.Status == StatusRunning {
		if fi, err := os.Stat(s.store.LogPath(name, id)); err == nil {
			offset = fi.Size()
		}
	}
	s.render(w, "run.html", map[string]any{
		"Run":    run,
		"Log":    logText,
		"Live":   run.Status == StatusRunning,
		"Offset": offset,
	})
}

func (s *Server) handleRaw(w http.ResponseWriter, r *http.Request) {
	name, id := r.PathValue("name"), r.PathValue("id")
	f, err := os.Open(s.store.LogPath(name, id))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	// ServeContent streams from the file (with Range support), never loading the
	// whole log into memory.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	http.ServeContent(w, r, "output.log", fi.ModTime(), f)
}

// handleStream tails the active run's log over Server-Sent Events. It re-reads
// appended bytes every 400ms, emits each complete line as a data event, and
// sends a final "end" event once the run reaches a terminal status.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	name, id := r.PathValue("name"), r.PathValue("id")
	if _, ok := s.cfg().Job(name); !ok {
		http.NotFound(w, r)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	logPath := s.store.LogPath(name, id)
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	ctx := r.Context()

	// Start from ?offset (the end-of-file the page already rendered) so we only
	// stream new output, not the whole log.
	var offset int64
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			offset = n
		}
	}
	const (
		sseChunk      = 1 << 20 // bytes read per tick (bounds memory per connection)
		sseFlushAfter = 64 << 10 // flush a newline-less buffer past this (e.g. \r progress)
	)
	var buf []byte
	emit := func(line string) { fmt.Fprintf(w, "data: %s\n\n", line) }

	for {
		if f, err := os.Open(logPath); err == nil {
			if _, err := f.Seek(offset, io.SeekStart); err == nil {
				chunk, _ := io.ReadAll(io.LimitReader(f, sseChunk))
				offset += int64(len(chunk))
				buf = append(buf, chunk...)
			}
			f.Close()
			for {
				i := bytes.IndexByte(buf, '\n')
				if i < 0 {
					break
				}
				emit(string(buf[:i]))
				buf = buf[i+1:]
			}
			// Don't let a long newline-less run (carriage-return progress bars)
			// grow the buffer unbounded — flush it as a partial line.
			if len(buf) > sseFlushAfter {
				emit(string(buf))
				buf = buf[:0]
			}
			flusher.Flush()
		}

		if run, err := s.store.ReadRun(name, id); err == nil && run.Status != StatusRunning {
			if len(buf) > 0 {
				emit(string(buf))
			}
			fmt.Fprint(w, "event: end\ndata: done\n\n")
			flusher.Flush()
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) handleRunNow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	job, ok := s.cfg().Job(name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.FormValue("force") == "1" {
		s.runner.ForceRun(job)
	} else {
		s.runner.Trigger(job, "manual")
	}
	redirectBack(w, r, "/job/"+name)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.cfg().Job(name); !ok {
		http.NotFound(w, r)
		return
	}
	s.runner.Cancel(name)
	redirectBack(w, r, "/job/"+name)
}

func (s *Server) handleToggle(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.cfg().Job(name); !ok {
		http.NotFound(w, r)
		return
	}
	s.runner.SetDisabled(name, !s.runner.IsDisabled(name))
	redirectBack(w, r, "/job/"+name)
}

// redirectBack returns the user to the page they acted from (same-host Referer),
// so a Run/Cancel/Disable click on the dashboard doesn't jump to the job view.
func redirectBack(w http.ResponseWriter, r *http.Request, fallback string) {
	if ref := r.Header.Get("Referer"); ref != "" {
		if u, err := url.Parse(ref); err == nil && (u.Host == "" || u.Host == r.Host) {
			http.Redirect(w, r, ref, http.StatusSeeOther)
			return
		}
	}
	http.Redirect(w, r, fallback, http.StatusSeeOther)
}

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	_ = s.Reload() // errors are logged; keep the old config on failure
	redirectBack(w, r, "/")
}

func (s *Server) handleDrain(on bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.runner.SetDraining(on)
		redirectBack(w, r, "/")
	}
}

func (s *Server) apiDrain(on bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.runner.SetDraining(on)
		writeJSON(w, http.StatusOK, map[string]any{"draining": on, "running": s.runner.RunningCount()})
	}
}

// handleMetrics exposes Prometheus-format metrics. Counters are over retained
// history (pruned), so they reset with retention — last_* gauges are exact.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfg()
	var b strings.Builder
	b.WriteString("# HELP cronkit_job_last_success Whether the last run succeeded (1) or not (0).\n# TYPE cronkit_job_last_success gauge\n")
	last := &strings.Builder{}
	dur := &strings.Builder{}
	exit := &strings.Builder{}
	ts := &strings.Builder{}
	runs := &strings.Builder{}
	running := &strings.Builder{}
	disabled := &strings.Builder{}
	pending := &strings.Builder{}
	dur.WriteString("# HELP cronkit_job_last_duration_seconds Duration of the last run.\n# TYPE cronkit_job_last_duration_seconds gauge\n")
	exit.WriteString("# HELP cronkit_job_last_exit_code Exit code of the last run.\n# TYPE cronkit_job_last_exit_code gauge\n")
	ts.WriteString("# HELP cronkit_job_last_run_timestamp_seconds Start time of the last run (unix).\n# TYPE cronkit_job_last_run_timestamp_seconds gauge\n")
	runs.WriteString("# HELP cronkit_job_runs_total Retained run count by status.\n# TYPE cronkit_job_runs_total counter\n")
	running.WriteString("# HELP cronkit_job_running Whether the job is currently running.\n# TYPE cronkit_job_running gauge\n")
	disabled.WriteString("# HELP cronkit_job_disabled Whether the job is disabled.\n# TYPE cronkit_job_disabled gauge\n")
	pending.WriteString("# HELP cronkit_job_pending Coalesced triggers awaiting a run.\n# TYPE cronkit_job_pending gauge\n")

	for _, j := range cfg.Jobs {
		ql := metricLabel(j.Name)
		counts := map[RunStatus]int{}
		for _, rr := range s.store.ListRuns(j.Name) {
			counts[rr.Status]++
		}
		if lr := s.store.LastRun(j.Name); lr != nil {
			ok := 0
			if lr.Status == StatusSuccess {
				ok = 1
			}
			fmt.Fprintf(last, "cronkit_job_last_success{job=\"%s\"} %d\n", ql, ok)
			fmt.Fprintf(dur, "cronkit_job_last_duration_seconds{job=\"%s\"} %g\n", ql, lr.Duration().Seconds())
			fmt.Fprintf(exit, "cronkit_job_last_exit_code{job=\"%s\"} %d\n", ql, lr.ExitCode)
			fmt.Fprintf(ts, "cronkit_job_last_run_timestamp_seconds{job=\"%s\"} %d\n", ql, lr.Start.Unix())
		}
		for _, st := range []RunStatus{StatusSuccess, StatusFailed, StatusTimeout, StatusCanceled, StatusInterrupted} {
			fmt.Fprintf(runs, "cronkit_job_runs_total{job=\"%s\",status=\"%s\"} %d\n", ql, st, counts[st])
		}
		rn := 0
		if s.runner.IsRunning(j.Name) {
			rn = 1
		}
		fmt.Fprintf(running, "cronkit_job_running{job=\"%s\"} %d\n", ql, rn)
		dis := 0
		if s.runner.IsDisabled(j.Name) {
			dis = 1
		}
		fmt.Fprintf(disabled, "cronkit_job_disabled{job=\"%s\"} %d\n", ql, dis)
		fmt.Fprintf(pending, "cronkit_job_pending{job=\"%s\"} %d\n", ql, s.runner.Waiting(j.Name).Count)
	}
	b.WriteString(last.String())
	b.WriteString(dur.String())
	b.WriteString(exit.String())
	b.WriteString(ts.String())
	b.WriteString(runs.String())
	b.WriteString(running.String())
	b.WriteString(disabled.String())
	b.WriteString(pending.String())

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, b.String())
}

// metricLabel escapes a job name for a Prometheus label value.
func metricLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return strings.ReplaceAll(s, "\n", `\n`)
}

func (s *Server) handleCSS(w http.ResponseWriter, r *http.Request) {
	b, err := uiFS.ReadFile("ui/style.css")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	_, _ = w.Write(b)
}

// --- settings (API key) -----------------------------------------------------

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	s.render(w, "settings.html", map[string]any{"APIKey": s.APIKey()})
}

func (s *Server) handleRotateKey(w http.ResponseWriter, r *http.Request) {
	s.RotateAPIKey()
	redirectBack(w, r, "/settings")
}

// --- integration API ---------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// withAPIKey rejects requests without a valid API key (Bearer or X-API-Key).
func (s *Server) withAPIKey(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.checkAPIKey(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="cronkit"`)
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
			return
		}
		h(w, r)
	}
}

func (s *Server) checkAPIKey(r *http.Request) bool {
	want := s.APIKey()
	if want == "" {
		return false
	}
	got := r.Header.Get("X-API-Key")
	if got == "" {
		if a := r.Header.Get("Authorization"); strings.HasPrefix(a, "Bearer ") {
			got = strings.TrimPrefix(a, "Bearer ")
		}
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *Server) apiListJobs(w http.ResponseWriter, r *http.Request) {
	type jobInfo struct {
		Name       string     `json:"name"`
		Schedule   string     `json:"schedule,omitempty"`
		Group      string     `json:"group,omitempty"`
		Enabled    bool       `json:"enabled"`
		Running    bool       `json:"running"`
		LastStatus string     `json:"last_status,omitempty"`
		LastRun    *time.Time `json:"last_run,omitempty"`
		Runs       int        `json:"runs"`
		Pending    int        `json:"pending,omitempty"`
		Blocked    bool       `json:"blocked,omitempty"`
	}
	out := []jobInfo{}
	for _, j := range s.cfg().Jobs {
		runs := s.store.ListRuns(j.Name)
		info := jobInfo{
			Name:     j.Name,
			Schedule: j.Schedule,
			Group:    j.Group,
			Enabled:  j.IsEnabled() && !s.runner.IsDisabled(j.Name),
			Running:  s.runner.IsRunning(j.Name),
			Runs:     len(runs),
			Pending:  s.runner.Waiting(j.Name).Count,
		}
		info.Blocked = info.Pending > 0 && !info.Running && s.runner.GroupBusy(j.Group)
		if len(runs) > 0 {
			info.LastStatus = string(runs[0].Status)
			t := runs[0].Start
			info.LastRun = &t
		}
		out = append(out, info)
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) apiRun(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	job, ok := s.cfg().Job(name)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "job not found"})
		return
	}
	if r.URL.Query().Get("force") == "1" {
		s.runner.ForceRun(job)
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "job": name, "status": "started"})
		return
	}
	res := s.runner.Trigger(job, "api")
	out := map[string]any{"ok": true, "job": name, "status": res.Action}
	if res.Action == "debounced" {
		out["fires_at"] = res.FiresAt.Format(time.RFC3339)
	}
	writeJSON(w, http.StatusAccepted, out)
}

func (s *Server) apiCancel(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.cfg().Job(name); !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "job not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "job": name, "canceled": s.runner.Cancel(name)})
}

func (s *Server) apiSetDisabled(disabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if _, ok := s.cfg().Job(name); !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "job not found"})
			return
		}
		s.runner.SetDisabled(name, disabled)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "job": name, "disabled": disabled})
	}
}

func (s *Server) handleFavicon(w http.ResponseWriter, r *http.Request) {
	b, err := uiFS.ReadFile("ui/favicon.svg")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(b)
}

// readLogTail returns at most max trailing bytes of a run's log.
func (s *Server) readLogTail(job, id string, max int64) string {
	f, err := os.Open(s.store.LogPath(job, id))
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
		b, _ := io.ReadAll(f)
		// Drop the partial first line after seeking mid-file.
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			b = b[i+1:]
		}
		return "…(truncated; see raw log)\n" + string(b)
	}
	b, _ := io.ReadAll(f)
	return string(b)
}

func (s *Server) fmttime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.In(s.loc()).Format("Jan 2 15:04:05")
}

// reltime renders a relative time ("5 min ago" / "in 2 hrs") with the precise
// timestamp as a hover tooltip. Accepts time.Time or *time.Time.
func (s *Server) reltime(v any) template.HTML {
	var t time.Time
	switch x := v.(type) {
	case time.Time:
		t = x
	case *time.Time:
		if x == nil {
			return template.HTML("—")
		}
		t = *x
	default:
		return template.HTML("—")
	}
	if t.IsZero() {
		return template.HTML("—")
	}
	abs := t.In(s.loc()).Format("Mon Jan 2, 2006 15:04:05 MST")
	return template.HTML(fmt.Sprintf(`<span class="rel" data-tip="%s">%s</span>`,
		template.HTMLEscapeString(abs), template.HTMLEscapeString(relWords(t))))
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}
