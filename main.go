// Command cronkit is a deliberately small cron scheduler with a web UI and
// per-execution logs. It runs shell commands on a schedule, captures each run's
// output/exit-code/duration to disk, and serves a tiny dashboard with a live
// log tail, run history and "run now". No database, no clustering, no plugins.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log.SetFlags(log.LstdFlags)
	var (
		configPath = flag.String("config", env("CRONKIT_CONFIG", "jobs.yml"), "path to the jobs config file")
		dataDir    = flag.String("data", env("CRONKIT_DATA", "./data"), "directory for run records and logs")
		addr       = flag.String("addr", env("CRONKIT_ADDR", ":8080"), "HTTP listen address")
	)
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	store, err := NewStore(*dataDir)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	if n := store.ReconcileRunning(); n > 0 {
		log.Printf("marked %d interrupted run(s) from a previous start", n)
	}

	cfgp := new(atomic.Pointer[Config])
	cfgp.Store(cfg)

	runner := NewRunner(cfgp, store)
	sched, err := NewScheduler(cfg, runner)
	if err != nil {
		log.Fatalf("scheduler: %v", err)
	}
	sched.Start()
	schedp := new(atomic.Pointer[Scheduler])
	schedp.Store(sched)
	defer func() { schedp.Load().Stop() }()

	srv, err := NewServer(cfgp, schedp, store, runner, *configPath)
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	// SIGHUP reloads the config in place (also available via POST /reload).
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			_ = srv.Reload()
		}
	}()

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Mux()}
	go func() {
		log.Printf("cronkit listening on %s — %d jobs, tz %s, keep %d runs", *addr, len(cfg.Jobs), cfg.Timezone, cfg.KeepRuns)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Print("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}
