package main

import (
	"strings"
	"testing"
	"time"
)

func observeCompletionTimes(milestones *flowFCTMilestones, count int) {
	// Reverse order verifies that finalization uses completion timestamps rather
	// than concurrent channel delivery order.
	for message := count; message >= 1; message-- {
		milestones.observe(time.Duration(message) * time.Millisecond)
	}
}

func TestNearestRankTarget(t *testing.T) {
	tests := []struct {
		name       string
		total      uint64
		percentile uint64
		want       uint64
	}{
		{name: "disabled", total: 0, percentile: 95, want: 0},
		{name: "p95", total: 3038, percentile: 95, want: 2887},
		{name: "p99", total: 3038, percentile: 99, want: 3008},
		{name: "p100", total: 3038, percentile: 100, want: 3038},
		{name: "single message", total: 1, percentile: 95, want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := nearestRankTarget(test.total, test.percentile); got != test.want {
				t.Fatalf("nearestRankTarget(%d, %d) = %d, want %d", test.total, test.percentile, got, test.want)
			}
		})
	}
}

func TestFlowFCTMilestonesUseTimestampOrderStatistics(t *testing.T) {
	milestones := newFlowFCTMilestones(3038, 18240512)
	observeCompletionTimes(&milestones, 3038)
	milestones.finalize()

	if milestones.p95.elapsed != 2887*time.Millisecond {
		t.Fatalf("p95 = %v, want 2.887s", milestones.p95.elapsed)
	}
	if milestones.p99.elapsed != 3008*time.Millisecond {
		t.Fatalf("p99 = %v, want 3.008s", milestones.p99.elapsed)
	}
	if milestones.p100.elapsed != 3038*time.Millisecond {
		t.Fatalf("p100 = %v, want 3.038s", milestones.p100.elapsed)
	}
	if status := milestones.status(3038, 18240512); status != "complete" {
		t.Fatalf("status = %q, want complete", status)
	}
}

func TestFlowFCTSummaryLine(t *testing.T) {
	milestones := newFlowFCTMilestones(3038, 18240512)
	observeCompletionTimes(&milestones, 3038)

	line := milestones.summaryLine(7, "192.0.2.1:6121", 3038, 18240512)
	for _, want := range []string{
		"[fct_summary] session=7 remote=192.0.2.1:6121",
		"expected_msgs=3038 received_msgs=3038",
		"expected_bytes=18240512 received_bytes=18240512",
		"fct95_s=2.887000 fct99_s=3.008000 fct100_s=3.038000",
		"method=nearest_rank metric=message_completion clock=monotonic status=complete",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("summary line %q does not contain %q", line, want)
		}
	}
}

func TestIncompleteFlowFCTSummaryUsesNA(t *testing.T) {
	milestones := newFlowFCTMilestones(3038, 18240512)
	observeCompletionTimes(&milestones, 2900)

	line := milestones.summaryLine(1, "192.0.2.2:6121", 2900, 17400000)
	if !strings.Contains(line, "fct95_s=2.887000 fct99_s=NA fct100_s=NA") {
		t.Fatalf("unexpected incomplete summary: %q", line)
	}
	if !strings.Contains(line, "status=incomplete") {
		t.Fatalf("unexpected incomplete status: %q", line)
	}
}

func TestCompleteMessageCountWithWrongByteCountIsRejected(t *testing.T) {
	milestones := newFlowFCTMilestones(3038, 18240512)
	observeCompletionTimes(&milestones, 3038)

	line := milestones.summaryLine(1, "192.0.2.3:6121", 3038, 18240511)
	if !strings.Contains(line, "status=byte_mismatch") {
		t.Fatalf("unexpected byte-mismatch status: %q", line)
	}
}
