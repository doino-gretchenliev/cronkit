package main

import (
	"strconv"
	"testing"
	"time"
)

func mkRun(t *testing.T, st *Store, job string, start time.Time, status RunStatus) *Run {
	t.Helper()
	r := &Run{
		ID:     strconv.FormatInt(start.UnixNano(), 10),
		Job:    job,
		Start:  start,
		End:    start.Add(time.Second),
		Status: status,
	}
	if err := st.writeMeta(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPruneByCount(t *testing.T) {
	st, _ := NewStore(t.TempDir())
	now := time.Now()
	for i := 0; i < 5; i++ {
		mkRun(t, st, "j", now.Add(time.Duration(i)*time.Second), StatusSuccess)
	}
	st.prune("j", 3, 0)
	if got := len(st.ListRuns("j")); got != 3 {
		t.Errorf("after prune count = %d, want 3", got)
	}
}

func TestPruneByDays(t *testing.T) {
	st, _ := NewStore(t.TempDir())
	now := time.Now()
	mkRun(t, st, "j", now.AddDate(0, 0, -10), StatusSuccess) // old
	mkRun(t, st, "j", now.Add(-time.Minute), StatusSuccess)  // recent
	st.prune("j", 100, 7)                                    // keep 7 days
	runs := st.ListRuns("j")
	if len(runs) != 1 {
		t.Fatalf("after day-prune = %d, want 1", len(runs))
	}
}

func TestReconcileRunning(t *testing.T) {
	st, _ := NewStore(t.TempDir())
	r := mkRun(t, st, "j", time.Now(), StatusRunning)
	if n := st.ReconcileRunning(); n != 1 {
		t.Fatalf("reconciled %d, want 1", n)
	}
	got, err := st.ReadRun("j", r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusInterrupted {
		t.Errorf("status = %s, want interrupted", got.Status)
	}
}
