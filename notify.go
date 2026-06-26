package main

import (
	"fmt"
	"net/smtp"
	"strings"
	"time"
)

// sendFailureEmail emails the configured recipients about a finished run via a
// plain SMTP relay (no auth — intended for an internal relay like the homelab's
// postfix). Best-effort: errors are returned for the caller to log.
func sendFailureEmail(n *Notify, job Job, r *Run, loc *time.Location, logTail string) error {
	if n == nil {
		return nil
	}
	subject := fmt.Sprintf("[cronkit] %s %s", job.Name, r.Status)
	var b strings.Builder
	fmt.Fprintf(&b, "Job:      %s\r\n", job.Name)
	fmt.Fprintf(&b, "Status:   %s\r\n", r.Status)
	fmt.Fprintf(&b, "Exit:     %d\r\n", r.ExitCode)
	fmt.Fprintf(&b, "Started:  %s\r\n", r.Start.In(loc).Format("Mon Jan 2, 2006 15:04:05 MST"))
	fmt.Fprintf(&b, "Duration: %s\r\n", humanDur(r.Duration()))
	fmt.Fprintf(&b, "Trigger:  %s\r\n", r.Trigger)
	fmt.Fprintf(&b, "Command:  %s\r\n", job.Command)
	if logTail != "" {
		fmt.Fprintf(&b, "\r\n--- last output ---\r\n%s\r\n", logTail)
	}

	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
		n.From, strings.Join(n.To, ", "), subject, b.String())

	addr := fmt.Sprintf("%s:%d", n.SMTPHost, n.SMTPPort)
	return smtp.SendMail(addr, nil, n.From, n.To, []byte(msg))
}
