package main

import (
	"testing"
	"time"

	"github.com/named-data/ndnd/std/datapacket"
)

func seedShadowLaterACKs(
	t *testing.T,
	detector *shadowLossDetector,
	base time.Time,
	laterACKs int,
) {
	t.Helper()
	detector.reset()
	detector.recordFreshSendLocked(1, base)
	for sequence := 2; sequence <= laterACKs+1; sequence++ {
		detector.recordFreshSendLocked(sequence, base.Add(time.Duration(sequence)*time.Millisecond))
	}
	for sequence := 2; sequence <= laterACKs+1; sequence++ {
		detector.recordDataLocked(sequence, base.Add(100*time.Millisecond))
	}
}

func findShadowSummary(
	t *testing.T,
	summaries []ShadowLossSummary,
	ageFloor time.Duration,
	laterACKThreshold int,
) ShadowLossSummary {
	t.Helper()
	for _, summary := range summaries {
		if summary.AgeFloor == ageFloor && summary.LaterACKThreshold == laterACKThreshold {
			return summary
		}
	}
	t.Fatalf("missing shadow summary for age=%v laterACKs=%d", ageFloor, laterACKThreshold)
	return ShadowLossSummary{}
}

func TestShadowDetectorRequiresBothAgeAndLaterACKEvidence(t *testing.T) {
	base := time.Unix(100, 0)
	var detector shadowLossDetector
	seedShadowLaterACKs(t, &detector, base, 8)

	detector.scanLocked(base.Add(249 * time.Millisecond))
	before := findShadowSummary(
		t, detector.summariesLocked(base.Add(249*time.Millisecond)), 250*time.Millisecond, 8,
	)
	if before.ShadowSuspects != 0 {
		t.Fatalf("candidate triggered before age floor: %+v", before)
	}

	detector.scanLocked(base.Add(250 * time.Millisecond))
	summaries := detector.summariesLocked(base.Add(250 * time.Millisecond))
	triggered := findShadowSummary(t, summaries, 250*time.Millisecond, 8)
	if triggered.ShadowSuspects != 1 || triggered.ActiveAtEnd != 1 {
		t.Fatalf("eligible candidate was not activated: %+v", triggered)
	}
	if summary := findShadowSummary(t, summaries, 250*time.Millisecond, 16); summary.ShadowSuspects != 0 {
		t.Fatalf("candidate ignored later-ACK threshold: %+v", summary)
	}
	if summary := findShadowSummary(t, summaries, 350*time.Millisecond, 8); summary.ShadowSuspects != 0 {
		t.Fatalf("candidate ignored age threshold: %+v", summary)
	}
}

func TestShadowDetectorClassifiesDataBeforeRTO(t *testing.T) {
	base := time.Unix(200, 0)
	var detector shadowLossDetector
	seedShadowLaterACKs(t, &detector, base, 8)
	detector.scanLocked(base.Add(250 * time.Millisecond))
	detector.recordDataLocked(1, base.Add(310*time.Millisecond))

	summary := findShadowSummary(
		t, detector.summariesLocked(base.Add(310*time.Millisecond)), 250*time.Millisecond, 8,
	)
	if summary.ShadowSuspects != 1 || summary.ResolvedBeforeRTO != 1 ||
		summary.ReachedRTO != 0 || summary.ActiveAtEnd != 0 {
		t.Fatalf("unexpected Data-before-RTO classification: %+v", summary)
	}
	if summary.SuspectInflightTotal != 60*time.Millisecond ||
		summary.SuspectInflightAvg != 60*time.Millisecond {
		t.Fatalf("unexpected suspect in-flight time: %+v", summary)
	}
}

func TestShadowDetectorMeasuresLeadToRTO(t *testing.T) {
	base := time.Unix(300, 0)
	var detector shadowLossDetector
	seedShadowLaterACKs(t, &detector, base, 8)
	detector.scanLocked(base.Add(250 * time.Millisecond))
	detector.recordRTOLocked(1, base.Add(430*time.Millisecond))

	summary := findShadowSummary(
		t, detector.summariesLocked(base.Add(430*time.Millisecond)), 250*time.Millisecond, 8,
	)
	if summary.ShadowSuspects != 1 || summary.ResolvedBeforeRTO != 0 ||
		summary.ReachedRTO != 1 || summary.ActiveAtEnd != 0 {
		t.Fatalf("unexpected RTO classification: %+v", summary)
	}
	if summary.FirstAttemptRTOs != 1 {
		t.Fatalf("first-attempt RTO count = %d, want 1", summary.FirstAttemptRTOs)
	}
	if summary.LeadToRTOTotal != 180*time.Millisecond ||
		summary.AvgLeadToRTO != 180*time.Millisecond ||
		summary.SuspectInflightTotal != 180*time.Millisecond {
		t.Fatalf("unexpected lead time: %+v", summary)
	}
}

func TestShadowDetectorCountsOnlyRTOBeforeRecovery(t *testing.T) {
	base := time.Unix(350, 0)
	var detector shadowLossDetector
	detector.reset()
	detector.recordFreshSendLocked(1, base)
	detector.recordRTOLocked(1, base.Add(300*time.Millisecond))
	detector.recordRTOLocked(1, base.Add(900*time.Millisecond))

	summary := findShadowSummary(
		t, detector.summariesLocked(base.Add(900*time.Millisecond)), 250*time.Millisecond, 8,
	)
	if summary.FirstAttemptRTOs != 1 {
		t.Fatalf("first-attempt RTO count = %d, want 1", summary.FirstAttemptRTOs)
	}
}

func TestShadowRTOBeforeEligibilityPreventsLaterCandidate(t *testing.T) {
	base := time.Unix(400, 0)
	var detector shadowLossDetector
	seedShadowLaterACKs(t, &detector, base, 32)
	detector.recordRTOLocked(1, base.Add(200*time.Millisecond))
	detector.scanLocked(base.Add(600 * time.Millisecond))

	for _, summary := range detector.summariesLocked(base.Add(600 * time.Millisecond)) {
		if summary.ShadowSuspects != 0 {
			t.Fatalf("post-RTO sequence became a shadow candidate: %+v", summary)
		}
	}
}

func TestFastRepairRequiresConfiguredAgeAndLaterACKs(t *testing.T) {
	base := time.Unix(500, 0)

	var tooYoung shadowLossDetector
	seedShadowLaterACKs(t, &tooYoung, base, shadowFastRepairLaterACKThreshold)
	tooYoung.recordAttemptLocked(1, 1)
	if candidates := tooYoung.scanIfDueLocked(
		base.Add(shadowFastRepairAgeFloor - time.Millisecond),
	); len(candidates) != 0 {
		t.Fatalf("fast repair triggered below age floor: %+v", candidates)
	}

	var tooFewACKs shadowLossDetector
	seedShadowLaterACKs(t, &tooFewACKs, base, shadowFastRepairLaterACKThreshold-1)
	tooFewACKs.recordAttemptLocked(1, 2)
	if candidates := tooFewACKs.scanIfDueLocked(
		base.Add(shadowFastRepairAgeFloor),
	); len(candidates) != 0 {
		t.Fatalf("fast repair triggered below later-ACK threshold: %+v", candidates)
	}

	var eligible shadowLossDetector
	seedShadowLaterACKs(t, &eligible, base, shadowFastRepairLaterACKThreshold)
	eligible.recordAttemptLocked(1, 3)
	candidates := eligible.scanIfDueLocked(base.Add(shadowFastRepairAgeFloor))
	if len(candidates) != 1 || candidates[0].Sequence != 1 ||
		candidates[0].Generation != 3 {
		t.Fatalf("eligible fast repair candidate = %+v, want sequence 1 generation 3", candidates)
	}
}

func TestShadowScanQueuesFastRepairThroughRetransmissionQueue(t *testing.T) {
	base := time.Unix(500, 0)
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	var firstAttempt ActiveAttempt
	for sequence := 1; sequence <= 9; sequence++ {
		name := flow.interestNameForSeq(sequence)
		if _, ok := flow.RecordInterestSend(
			name, sequence, base.Add(time.Duration(sequence-1)*time.Millisecond), false,
		); !ok {
			t.Fatalf("fresh send %d rejected", sequence)
		}
		attempt, ok := controller.Add(name, flow.Prefix, sequence, flow, ModeDT)
		if !ok {
			t.Fatalf("attempt %d rejected", sequence)
		}
		if sequence == 1 {
			firstAttempt = attempt
		}
	}

	flow.mu.Lock()
	for sequence := 2; sequence <= 9; sequence++ {
		flow.shadowLoss.recordDataLocked(sequence, base.Add(100*time.Millisecond))
		delete(flow.pendingSends, flow.interestNameForSeq(sequence))
	}
	flow.mu.Unlock()
	flow.runShadowLossScan(base.Add(shadowFastRepairAgeFloor), controller)

	queuedSequence, ok := flow.PopRetransmission()
	if !ok || queuedSequence != 1 {
		t.Fatalf("queued retransmission = (%d, %v), want (1, true)", queuedSequence, ok)
	}
	flow.runShadowLossScan(
		base.Add(shadowFastRepairAgeFloor+shadowLossScanInterval), controller,
	)
	if flow.HasQueuedRetransmission() {
		t.Fatal("repeated scan queued a second repair for the same generation")
	}
	if !controller.IsCurrent(flow.interestNameForSeq(1), firstAttempt.Generation) {
		t.Fatal("fast repair queueing replaced the active attempt before paced send")
	}

	summary := flow.GetShadowFastRepairSummary()
	if summary.Triggers != 1 || summary.Queued != 1 || summary.Sent != 0 {
		t.Fatalf("unexpected fast repair summary: %+v", summary)
	}
}

func TestFastRepairControllerAllowsAtMostOnePerGeneration(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	name := recordTestSend(t, flow, 11, false)
	attempt, ok := controller.Add(name, flow.Prefix, 11, flow, ModeDT)
	if !ok {
		t.Fatal("Add rejected")
	}
	candidate := shadowFastRepairCandidate{Sequence: 11, Generation: attempt.Generation}

	if outcome := controller.QueueShadowFastRepairIfCurrent(name, candidate, flow); outcome != shadowFastRepairQueueQueued {
		t.Fatalf("first fast repair outcome = %v, want queued", outcome)
	}
	if sequence, ok := flow.PopRetransmission(); !ok || sequence != 11 {
		t.Fatalf("first queue result = (%d, %v), want (11, true)", sequence, ok)
	}
	if outcome := controller.QueueShadowFastRepairIfCurrent(name, candidate, flow); outcome != shadowFastRepairQueueAlreadyTriggered {
		t.Fatalf("second fast repair outcome = %v, want already-triggered", outcome)
	}
	if flow.HasQueuedRetransmission() {
		t.Fatal("second trigger queued another retransmission")
	}

	summary := flow.GetShadowFastRepairSummary()
	if summary.Queued != 1 || summary.AlreadyTriggered != 1 {
		t.Fatalf("unexpected de-duplication counters: %+v", summary)
	}
}

func TestFastRepairDataRaceDoesNotQueueStaleGeneration(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	name := recordTestSend(t, flow, 12, false)
	attempt, ok := controller.Add(name, flow.Prefix, 12, flow, ModeDT)
	if !ok {
		t.Fatal("Add rejected")
	}
	candidate := shadowFastRepairCandidate{Sequence: 12, Generation: attempt.Generation}
	payload, err := datapacket.NewDataPacket().SerializeData()
	if err != nil {
		t.Fatalf("SerializeData: %v", err)
	}
	if !flow.OnData(payload, name, time.Now()) {
		t.Fatal("Data was not accepted")
	}
	controller.Complete(name)

	if outcome := controller.QueueShadowFastRepairIfCurrent(name, candidate, flow); outcome != shadowFastRepairQueueStale {
		t.Fatalf("post-Data fast repair outcome = %v, want stale", outcome)
	}
	if flow.HasQueuedRetransmission() {
		t.Fatal("post-Data candidate queued a stale retransmission")
	}
}

func TestFastRepairLeavesControlStateAndRTOUntouched(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	name := recordTestSend(t, flow, 13, false)
	attempt, ok := controller.Add(name, flow.Prefix, 13, flow, ModeDT)
	if !ok {
		t.Fatal("Add rejected")
	}
	epochStart := time.Unix(700, 0)
	lastRateUpdate := time.Unix(710, 0)
	flow.currentRate = 321
	flow.congestionWindow = 47
	flow.rto = 789
	flow.timeoutEpochStart = epochStart
	flow.timeoutEpochEvents = 2
	flow.timeoutEpochBackoff = false
	flow.timeoutEvents = 4
	flow.timeoutBackoffs = 0
	flow.interestsSentInPeriod = 23
	flow.lastRateUpdate = lastRateUpdate
	candidate := shadowFastRepairCandidate{Sequence: 13, Generation: attempt.Generation}

	if outcome := controller.QueueShadowFastRepairIfCurrent(name, candidate, flow); outcome != shadowFastRepairQueueQueued {
		t.Fatalf("fast repair outcome = %v, want queued", outcome)
	}
	if flow.currentRate != 321 || flow.congestionWindow != 47 || flow.rto != 789 ||
		!flow.timeoutEpochStart.Equal(epochStart) || flow.timeoutEpochEvents != 2 ||
		flow.timeoutEpochBackoff ||
		flow.timeoutEvents != 4 || flow.timeoutBackoffs != 0 ||
		flow.interestsSentInPeriod != 23 || !flow.lastRateUpdate.Equal(lastRateUpdate) {
		t.Fatalf(
			"control state changed: rate=%v cwnd=%v rto=%v epoch=%v/%d/%v timeouts=%d backoffs=%d sent=%d lastUpdate=%v",
			flow.currentRate, flow.congestionWindow, flow.rto,
			flow.timeoutEpochStart, flow.timeoutEpochEvents, flow.timeoutEpochBackoff,
			flow.timeoutEvents, flow.timeoutBackoffs, flow.interestsSentInPeriod,
			flow.lastRateUpdate,
		)
	}
}

func TestFastRepairDoesNotDuplicateAnExistingRecoveryQueueEntry(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	name := recordTestSend(t, flow, 15, false)
	attempt, ok := controller.Add(name, flow.Prefix, 15, flow, ModeDT)
	if !ok {
		t.Fatal("Add rejected")
	}
	if !controller.QueueRetransmissionIfCurrent(name, attempt.Generation) {
		t.Fatal("normal recovery path did not queue the sequence")
	}
	candidate := shadowFastRepairCandidate{Sequence: 15, Generation: attempt.Generation}
	if outcome := controller.QueueShadowFastRepairIfCurrent(name, candidate, flow); outcome != shadowFastRepairQueueAlreadyQueued {
		t.Fatalf("fast repair outcome = %v, want already-queued", outcome)
	}
	if sequence, ok := flow.PopRetransmission(); !ok || sequence != 15 {
		t.Fatalf("queue result = (%d, %v), want one sequence 15", sequence, ok)
	}
	if flow.HasQueuedRetransmission() {
		t.Fatal("fast repair duplicated an existing recovery queue entry")
	}
	recordTestSend(t, flow, 15, true)
	if _, ok := controller.Add(name, flow.Prefix, 15, flow, ModeDT); !ok {
		t.Fatal("normal recovery send Add rejected")
	}
	if summary := flow.GetShadowFastRepairSummary(); summary.Sent != 0 {
		t.Fatalf("normal recovery was counted as a fast repair send: %+v", summary)
	}
}

func TestNormalRetransmissionRemainsAvailableAfterFastRepairSend(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	name := recordTestSend(t, flow, 14, false)
	first, ok := controller.Add(name, flow.Prefix, 14, flow, ModeDT)
	if !ok {
		t.Fatal("first Add rejected")
	}
	candidate := shadowFastRepairCandidate{Sequence: 14, Generation: first.Generation}
	if outcome := controller.QueueShadowFastRepairIfCurrent(name, candidate, flow); outcome != shadowFastRepairQueueQueued {
		t.Fatalf("fast repair outcome = %v, want queued", outcome)
	}
	if sequence, ok := flow.PopRetransmission(); !ok || sequence != 14 {
		t.Fatalf("fast repair queue result = (%d, %v), want (14, true)", sequence, ok)
	}

	recordTestSend(t, flow, 14, true)
	second, ok := controller.Add(name, flow.Prefix, 14, flow, ModeDT)
	if !ok || second.Generation == first.Generation {
		t.Fatalf("second attempt = %+v, ok=%v", second, ok)
	}
	if !controller.QueueRetransmissionIfCurrent(name, second.Generation) {
		t.Fatal("normal RTO path could not queue the current repaired generation")
	}
	if sequence, ok := flow.PopRetransmission(); !ok || sequence != 14 {
		t.Fatalf("normal retransmission queue result = (%d, %v), want (14, true)", sequence, ok)
	}

	summary := flow.GetShadowFastRepairSummary()
	if summary.Sent != 1 {
		t.Fatalf("fast repair sent count = %d, want 1", summary.Sent)
	}
}
