package main

import (
	"fmt"
	"html/template"
	"sort"
	"strings"
	"time"
)

// humanDur renders a duration in a human-readable form at any scale:
// "250ms", "3.4s", "45s", "3m 20s", "1h 15m", "2d 3h".
func humanDur(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < 10*time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm %ds", int(d/time.Minute), int(d/time.Second)%60)
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh %dm", int(d/time.Hour), int(d/time.Minute)%60)
	default:
		return fmt.Sprintf("%dd %dh", int(d/(24*time.Hour)), int(d/time.Hour)%24)
	}
}

// relWords renders a time relative to now: "just now", "5 min ago", "in 2 hrs".
func relWords(t time.Time) string {
	d := time.Until(t)
	future := d >= 0
	a := d
	if a < 0 {
		a = -a
	}
	if a < time.Second {
		return "just now"
	}
	var n int
	var unit string
	switch {
	case a < time.Minute:
		n, unit = int(a/time.Second), "sec"
	case a < time.Hour:
		n, unit = int(a/time.Minute), "min"
	case a < 24*time.Hour:
		n, unit = int(a/time.Hour), "hr"
	default:
		n, unit = int(a/(24*time.Hour)), "day"
	}
	if n != 1 {
		unit += "s"
	}
	if future {
		return fmt.Sprintf("in %d %s", n, unit)
	}
	return fmt.Sprintf("%d %s ago", n, unit)
}

// progressBar renders a mini progress bar (elapsed vs average run time).
func progressBar(pct int, overdue bool, avg time.Duration) template.HTML {
	cls := "pbar"
	title := fmt.Sprintf("%d%% of ~%s average", pct, humanDur(avg))
	if overdue {
		cls += " over"
		title = fmt.Sprintf("over the ~%s average", humanDur(avg))
	}
	return template.HTML(fmt.Sprintf(`<div class="%s" title="%s"><span style="width:%d%%"></span></div>`,
		cls, template.HTMLEscapeString(title), pct))
}

// avgRunDuration averages the durations of completed runs (newest-first slice,
// most recent `limit`). Returns 0 if none.
func avgRunDuration(runs []*Run, limit int) time.Duration {
	var sum time.Duration
	var n int
	for _, r := range runs {
		if r.Status == StatusRunning || r.End.IsZero() {
			continue
		}
		sum += r.End.Sub(r.Start)
		if n++; n >= limit {
			break
		}
	}
	if n == 0 {
		return 0
	}
	return sum / time.Duration(n)
}

// statusClass maps a run status to a CSS class for the status badge.
func statusClass(st RunStatus) string {
	switch st {
	case StatusSuccess:
		return "ok"
	case StatusFailed:
		return "fail"
	case StatusTimeout:
		return "warn"
	case StatusCanceled:
		return "canceled"
	case StatusInterrupted:
		return "interrupted"
	case StatusRunning:
		return "running"
	default:
		return "none"
	}
}

// sparkline renders a small inline-SVG bar chart of recent run durations.
// runs are newest-first; bars are drawn oldest→newest, colored by status.
func sparkline(runs []*Run) template.HTML {
	const (
		maxBars = 30
		barW    = 7
		gap     = 2
		h       = 36
	)
	if len(runs) == 0 {
		return template.HTML(`<span class="muted">no runs yet</span>`)
	}
	// Take the newest maxBars, then reverse to chronological order.
	if len(runs) > maxBars {
		runs = runs[:maxBars]
	}
	ordered := make([]*Run, len(runs))
	for i, r := range runs {
		ordered[len(runs)-1-i] = r
	}

	var maxD time.Duration
	for _, r := range ordered {
		if d := r.Duration(); d > maxD {
			maxD = d
		}
	}
	if maxD <= 0 {
		maxD = time.Second
	}

	w := len(ordered)*(barW+gap) - gap
	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="spark" width="%d" height="%d" viewBox="0 0 %d %d" role="img">`, w, h, w, h)
	for i, r := range ordered {
		bh := int(float64(h-2) * float64(r.Duration()) / float64(maxD))
		if bh < 2 {
			bh = 2
		}
		x := i * (barW + gap)
		y := h - bh
		title := fmt.Sprintf("%s · %s · exit %d", r.Status, humanDur(r.Duration()), r.ExitCode)
		fmt.Fprintf(&b, `<rect class="bar %s" x="%d" y="%d" width="%d" height="%d"><title>%s</title></rect>`,
			statusClass(r.Status), x, y, barW, bh, template.HTMLEscapeString(title))
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

// chainDiagram renders the chain graph centered on focus: direct upstream
// parents (jobs that chain to it) on the left, the focus job, then the full
// downstream chain (BFS, cycle-guarded) to the right. Edges are green for
// on_success, red for on_failure; nodes link to their job page. Returns "" when
// the job has no chain links.
func chainDiagram(cfg *Config, focus Job) template.HTML {
	type node struct {
		name       string
		level, row int
		isFocus    bool
	}
	type edge struct{ from, to, kind string } // kind: success|failure

	nodes := map[string]*node{focus.Name: {name: focus.Name, isFocus: true}}
	var edges []edge
	seenEdge := map[string]bool{}
	addEdge := func(from, to, kind string) {
		k := from + ">" + to + ":" + kind
		if !seenEdge[k] {
			seenEdge[k] = true
			edges = append(edges, edge{from, to, kind})
		}
	}

	// Downstream BFS from the focus job.
	type qi struct {
		name  string
		level int
	}
	queue := []qi{{focus.Name, 0}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		j, ok := cfg.Job(cur.name)
		if !ok || cur.level >= maxChainDepth {
			continue
		}
		step := func(targets []string, kind string) {
			for _, t := range targets {
				addEdge(cur.name, t, kind)
				if _, exists := nodes[t]; !exists {
					nodes[t] = &node{name: t, level: cur.level + 1}
					queue = append(queue, qi{t, cur.level + 1})
				}
			}
		}
		step(j.OnSuccess, "success")
		step(j.OnFailure, "failure")
	}

	// Direct upstream parents (one level): jobs that chain into the focus job.
	for _, j := range cfg.Jobs {
		parent := func(targets []string, kind string) {
			for _, t := range targets {
				if t != focus.Name {
					continue
				}
				if _, exists := nodes[j.Name]; !exists {
					nodes[j.Name] = &node{name: j.Name, level: -1}
				}
				addEdge(j.Name, focus.Name, kind)
			}
		}
		parent(j.OnSuccess, "success")
		parent(j.OnFailure, "failure")
	}

	if len(edges) == 0 {
		return ""
	}

	// Bucket nodes per level, order each column by name, find extents.
	byLevel := map[int][]*node{}
	minL, maxL, maxRows := 0, 0, 0
	for _, n := range nodes {
		byLevel[n.level] = append(byLevel[n.level], n)
		minL, maxL = min(minL, n.level), max(maxL, n.level)
	}
	for lvl := minL; lvl <= maxL; lvl++ {
		ns := byLevel[lvl]
		sort.Slice(ns, func(i, j int) bool { return ns[i].name < ns[j].name })
		for i, n := range ns {
			n.row = i
		}
		maxRows = max(maxRows, len(ns))
	}

	const (
		boxW, boxH    = 150, 36
		colGap, rowGap = 70, 22
		padX, padY    = 12, 12
	)
	colW, rowH := boxW+colGap, boxH+rowGap
	cols := maxL - minL + 1
	width := padX*2 + cols*boxW + (cols-1)*colGap
	height := padY*2 + maxRows*boxH + max(0, maxRows-1)*rowGap

	xOf := func(level int) int { return padX + (level-minL)*colW }
	yOf := func(n *node) int {
		count := len(byLevel[n.level])
		totalH := count*boxH + max(0, count-1)*rowGap
		startY := padY + (height-padY*2-totalH)/2
		return startY + n.row*rowH
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="chain" width="%d" height="%d" viewBox="0 0 %d %d" role="img">`, width, height, width, height)
	b.WriteString(`<defs>` +
		`<marker id="ah-s" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 Z" fill="var(--ok)"/></marker>` +
		`<marker id="ah-f" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 Z" fill="var(--fail)"/></marker>` +
		`</defs>`)

	for _, e := range edges {
		from, to := nodes[e.from], nodes[e.to]
		x1, y1 := xOf(from.level)+boxW, yOf(from)+boxH/2
		x2, y2 := xOf(to.level), yOf(to)+boxH/2
		mx := (x1 + x2) / 2
		color, marker := "var(--ok)", "ah-s"
		if e.kind == "failure" {
			color, marker = "var(--fail)", "ah-f"
		}
		fmt.Fprintf(&b, `<path class="edge" d="M%d,%d C%d,%d %d,%d %d,%d" stroke="%s" fill="none" marker-end="url(#%s)"/>`,
			x1, y1, mx, y1, mx, y2, x2, y2, color, marker)
	}
	for _, n := range nodes {
		x, y := xOf(n.level), yOf(n)
		cls := "node"
		if n.isFocus {
			cls = "node focus"
		}
		esc := template.HTMLEscapeString(n.name)
		fmt.Fprintf(&b, `<a href="/job/%s"><rect class="%s" x="%d" y="%d" width="%d" height="%d" rx="6"/>`+
			`<text x="%d" y="%d" text-anchor="middle" dominant-baseline="central">%s</text></a>`,
			esc, cls, x, y, boxW, boxH, x+boxW/2, y+boxH/2, esc)
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}
