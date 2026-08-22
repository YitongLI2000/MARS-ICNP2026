package main

import (
	"testing"
	"time"

	"github.com/named-data/ndnd/std/datapacket"
)

func TestRTO1Configuration(t *testing.T) {
	if rtoOuterMultiplier != 1 {
		t.Fatalf("rtoOuterMultiplier = %d, want 1", rtoOuterMultiplier)
	}

	const calculatedRTO = 300.0
	if got := applyRTOOuterMultiplier(calculatedRTO, rtoOuterMultiplier); got != calculatedRTO {
		t.Fatalf("RTO1 multiplier: got %v, want %v", got, calculatedRTO)
	}
	if got := applyRTOOuterMultiplier(10, rtoOuterMultiplier); got != MIN_RTO {
		t.Fatalf("minimum clamp = %v, want %v", got, MIN_RTO)
	}
	if got := applyRTOOuterMultiplier(MAX_RTO+1, rtoOuterMultiplier); got != MAX_RTO {
		t.Fatalf("maximum clamp = %v, want %v", got, MAX_RTO)
	}
}

func newDTTestFlow(prefix string) *FlowContext {
	f := &FlowContext{
		Prefix:      prefix,
		Mode:        ModeDT,
		doneCh:      make(chan struct{}),
		resetSignal: make(chan struct{}, 1),
	}
	f.initDTState()
	return f
}

func recordTestSend(t *testing.T, f *FlowContext, seq int, retransmission bool) string {
	t.Helper()
	name := f.interestNameForSeq(seq)
	mode, ok := f.RecordInterestSend(name, seq, time.Now(), retransmission)
	if !ok {
		t.Fatalf("RecordInterestSend(%q) rejected", name)
	}
	if mode != ModeDT {
		t.Fatalf("RecordInterestSend mode = %v, want ModeDT", mode)
	}
	return name
}

func TestRemoveIfCurrentProtectsNewGeneration(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	name := recordTestSend(t, flow, 1, false)

	first, ok := controller.Add(name, flow.Prefix, 1, flow, ModeDT)
	if !ok {
		t.Fatal("first Add rejected")
	}
	recordTestSend(t, flow, 1, true)
	second, ok := controller.Add(name, flow.Prefix, 1, flow, ModeDT)
	if !ok {
		t.Fatal("second Add rejected")
	}

	if second.Generation <= first.Generation {
		t.Fatalf("generation did not increase: first=%d second=%d", first.Generation, second.Generation)
	}
	if second.Attempt != first.Attempt+1 {
		t.Fatalf("attempt did not increase: first=%d second=%d", first.Attempt, second.Attempt)
	}
	if first.RTOBackoffLevel != 0 || second.RTOBackoffLevel != 1 {
		t.Fatalf(
			"RTO backoff levels = first %d second %d, want 0 and 1",
			first.RTOBackoffLevel, second.RTOBackoffLevel,
		)
	}
	if controller.RemoveIfCurrent(name, first.Generation) {
		t.Fatal("old generation removed the current attempt")
	}
	if !controller.IsCurrent(name, second.Generation) {
		t.Fatal("current generation disappeared after stale removal")
	}
	if !controller.RemoveIfCurrent(name, second.Generation) {
		t.Fatal("current generation could not remove itself")
	}
}

func TestCurrentDTNackQueuesSameSequence(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	const seq = 27
	name := recordTestSend(t, flow, seq, false)
	attempt, ok := controller.Add(name, flow.Prefix, seq, flow, ModeDT)
	if !ok {
		t.Fatal("Add rejected")
	}

	if !controller.QueueRetransmissionIfCurrent(name, attempt.Generation) {
		t.Fatal("current DT attempt did not queue a retransmission")
	}
	queuedSeq, ok := flow.PopRetransmission()
	if !ok || queuedSeq != seq {
		t.Fatalf("queued retransmission = (%d, %v), want (%d, true)", queuedSeq, ok, seq)
	}
	if !controller.IsCurrent(name, attempt.Generation) {
		t.Fatal("queueing a DT retransmission cleared the logical request")
	}
}

func TestOldGenerationCannotQueueRetransmission(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	controller := NewRetransmissionController(nil, &Statistics{})
	name := recordTestSend(t, flow, 9, false)
	first, _ := controller.Add(name, flow.Prefix, 9, flow, ModeDT)
	recordTestSend(t, flow, 9, true)
	second, _ := controller.Add(name, flow.Prefix, 9, flow, ModeDT)

	if controller.QueueRetransmissionIfCurrent(name, first.Generation) {
		t.Fatal("old generation queued a retransmission")
	}
	if !controller.QueueRetransmissionIfCurrent(name, second.Generation) {
		t.Fatal("current generation failed to queue a retransmission")
	}
}

func TestRetransmissionTimeoutBackoffJitterAndBounds(t *testing.T) {
	const baseRTO = 300.0
	previous := time.Duration(0)
	for backoffLevel := 0; backoffLevel <= 4; backoffLevel++ {
		timeout := retransmissionTimeout(baseRTO, backoffLevel, "/pro0app/con0app/dt/seq-1", uint64(backoffLevel+1))
		if timeout < time.Duration(MIN_RTO*float64(time.Millisecond)) {
			t.Fatalf("backoff level %d timeout %v is below MIN_RTO", backoffLevel, timeout)
		}
		if timeout > time.Duration(MAX_RTO*float64(time.Millisecond)) {
			t.Fatalf("backoff level %d timeout %v exceeds MAX_RTO", backoffLevel, timeout)
		}
		if backoffLevel > 0 && timeout <= previous {
			t.Fatalf("backoff did not increase at level %d: previous=%v current=%v", backoffLevel, previous, timeout)
		}
		previous = timeout
	}

	seen := make(map[time.Duration]bool)
	belowCap := false
	for generation := uint64(1); generation <= 32; generation++ {
		timeout := retransmissionTimeout(MAX_RTO, 20, "/pro0app/con0app/dt/seq-1", generation)
		if timeout < time.Duration(MIN_RTO*float64(time.Millisecond)) ||
			timeout > time.Duration(MAX_RTO*float64(time.Millisecond)) {
			t.Fatalf("capped timeout %v is outside configured bounds", timeout)
		}
		seen[timeout] = true
		belowCap = belowCap || timeout < time.Duration(MAX_RTO*float64(time.Millisecond))
	}
	if len(seen) < 2 || !belowCap {
		t.Fatalf("capped attempts lost jitter: distinct=%d belowCap=%v", len(seen), belowCap)
	}
}

func TestFastRepairDoesNotAdvanceRTOBackoffLevel(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	flow.rto = 300
	controller := NewRetransmissionController(nil, &Statistics{})
	const seq = 31
	name := recordTestSend(t, flow, seq, false)
	first, ok := controller.Add(name, flow.Prefix, seq, flow, ModeDT)
	if !ok {
		t.Fatal("first Add rejected")
	}
	if first.Attempt != 1 || first.RTOBackoffLevel != 0 {
		t.Fatalf("first attempt = %+v, want attempt 1 backoff level 0", first)
	}

	candidate := shadowFastRepairCandidate{Sequence: seq, Generation: first.Generation}
	if outcome := controller.QueueShadowFastRepairIfCurrent(name, candidate, flow); outcome != shadowFastRepairQueueQueued {
		t.Fatalf("fast repair outcome = %v, want queued", outcome)
	}
	if queuedSeq, popped := flow.PopRetransmission(); !popped || queuedSeq != seq {
		t.Fatalf("fast repair queue result = (%d, %v), want (%d, true)", queuedSeq, popped, seq)
	}
	recordTestSend(t, flow, seq, true)
	fastRepair, ok := controller.Add(name, flow.Prefix, seq, flow, ModeDT)
	if !ok {
		t.Fatal("fast-repair Add rejected")
	}
	if fastRepair.Attempt != 2 || fastRepair.RTOBackoffLevel != 0 {
		t.Fatalf("fast repair attempt = %+v, want attempt 2 backoff level 0", fastRepair)
	}
	wantFastTimeout := retransmissionTimeout(
		flow.rto, 0, name, fastRepair.Generation,
	)
	if fastRepair.Timeout != wantFastTimeout {
		t.Fatalf("fast repair timeout = %v, want %v", fastRepair.Timeout, wantFastTimeout)
	}

	if !controller.QueueRetransmissionIfCurrent(name, fastRepair.Generation) {
		t.Fatal("normal recovery after fast repair was not queued")
	}
	if queuedSeq, popped := flow.PopRetransmission(); !popped || queuedSeq != seq {
		t.Fatalf("normal recovery queue result = (%d, %v), want (%d, true)", queuedSeq, popped, seq)
	}
	recordTestSend(t, flow, seq, true)
	normalRecovery, ok := controller.Add(name, flow.Prefix, seq, flow, ModeDT)
	if !ok {
		t.Fatal("normal-recovery Add rejected")
	}
	if normalRecovery.Attempt != 3 || normalRecovery.RTOBackoffLevel != 1 {
		t.Fatalf("normal recovery attempt = %+v, want attempt 3 backoff level 1", normalRecovery)
	}
	wantRecoveryTimeout := retransmissionTimeout(
		flow.rto, 1, name, normalRecovery.Generation,
	)
	if normalRecovery.Timeout != wantRecoveryTimeout {
		t.Fatalf("normal recovery timeout = %v, want %v", normalRecovery.Timeout, wantRecoveryTimeout)
	}
}

func TestDTRTONeverChangesRateWindowOrControlEpoch(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	flow.currentRate = 100
	flow.estimatedBandwidth = 120
	flow.baseRTT = 50
	flow.rto = 300
	flow.congestionWindow = 40
	flow.interestsSentInPeriod = 17
	flow.lastRateUpdate = time.Unix(90, 0)
	now := time.Unix(100, 0)

	for timeout := 0; timeout < 7; timeout++ {
		rate, rto, backedOff := flow.OnDTRTO(
			now.Add(time.Duration(timeout)*10*time.Millisecond),
			300*time.Millisecond,
		)
		if backedOff || rate != 100 || rto != 300 {
			t.Fatalf(
				"timeout %d = rate %v rto %v backedOff=%v; want 100, 300, false",
				timeout+1, rate, rto, backedOff,
			)
		}
	}

	if flow.currentRate != 100 || flow.congestionWindow != 40 {
		t.Fatalf("control state changed: rate=%v cwnd=%v", flow.currentRate, flow.congestionWindow)
	}
	if flow.interestsSentInPeriod != 17 || !flow.lastRateUpdate.Equal(time.Unix(90, 0)) {
		t.Fatalf(
			"rate-control epoch changed: sent=%d lastUpdate=%v",
			flow.interestsSentInPeriod, flow.lastRateUpdate,
		)
	}
	if flow.timeoutEvents != 7 || flow.timeoutBackoffs != 0 {
		t.Fatalf("timeout counters = events %d backoffs %d, want 7 and 0", flow.timeoutEvents, flow.timeoutBackoffs)
	}
}

func TestDTInflightLimitUsesAckClockedWindow(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	flow.congestionWindow = 23.25
	limit := flow.dtInflightLimitLocked()
	if limit != 24 {
		t.Fatalf("in-flight limit = %d, want 24", limit)
	}
	for seq := 1; seq <= limit; seq++ {
		flow.pendingSends[flow.interestNameForSeq(seq)] = PendingSend{}
	}
	if flow.CanSendNewDT() {
		t.Fatal("flow admitted new sequence at its in-flight limit")
	}
	delete(flow.pendingSends, flow.interestNameForSeq(limit))
	if !flow.CanSendNewDT() {
		t.Fatal("flow did not admit new sequence below its in-flight limit")
	}
}

func TestDTCongestionWindowGrowsFromAcknowledgements(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	flow.congestionWindow = 20
	for i := 0; i < 20; i++ {
		flow.growCongestionWindowOnAckLocked()
	}
	if flow.congestionWindow <= 20 || flow.congestionWindow >= 22 {
		t.Fatalf("ACK-grown congestion window = %v, want (20, 22)", flow.congestionWindow)
	}
}

func TestPDAddKeepsDiscoveryRTO(t *testing.T) {
	flow := &FlowContext{
		Prefix:       "/pro0app",
		Mode:         ModePD,
		rto:          PD_INIT_RTO,
		pendingSends: make(map[string]PendingSend),
		doneCh:       make(chan struct{}),
		resetSignal:  make(chan struct{}, 1),
	}
	name := "/pro0app/con0app/pd/0/seq-1"
	mode, ok := flow.RecordInterestSend(name, 1, time.Now(), false)
	if !ok {
		t.Fatal("RecordInterestSend rejected PD Interest")
	}
	controller := NewRetransmissionController(nil, &Statistics{})
	attempt, ok := controller.Add(name, flow.Prefix, 1, flow, mode)
	if !ok {
		t.Fatal("Add rejected PD Interest")
	}
	want := time.Duration(PD_INIT_RTO * float64(time.Millisecond))
	if attempt.Timeout != want {
		t.Fatalf("PD timeout = %v, want %v", attempt.Timeout, want)
	}
}

func TestIncompleteFinalStatsKeepFCT100Zero(t *testing.T) {
	originalMaxPackets := MAX_PACKETS
	MAX_PACKETS = 3
	defer func() { MAX_PACKETS = originalMaxPackets }()

	flow := newDTTestFlow("/pro0app")
	flow.pktsReceivedTotal = 2
	flow.totalBytes = 100
	flow.endTime = flow.startTime.Add(time.Second)
	flow.p100Time = flow.endTime
	stats := flow.GetFinalStats()

	if stats.Complete || stats.ReceivedPackets != 2 || stats.ExpectedPackets != 3 || stats.MissingPackets != 1 {
		t.Fatalf("unexpected incomplete stats: %+v", stats)
	}
	if stats.FCT100 != 0 {
		t.Fatalf("incomplete FCT100 = %v, want 0", stats.FCT100)
	}
}

func TestCompleteFinalStats(t *testing.T) {
	originalMaxPackets := MAX_PACKETS
	MAX_PACKETS = 3
	defer func() { MAX_PACKETS = originalMaxPackets }()

	flow := newDTTestFlow("/pro0app")
	flow.pktsReceivedTotal = 3
	flow.isFinished = true
	flow.p100Time = flow.startTime.Add(2 * time.Second)
	flow.endTime = flow.p100Time
	stats := flow.GetFinalStats()

	if !stats.Complete || stats.ReceivedPackets != 3 || stats.ExpectedPackets != 3 || stats.MissingPackets != 0 {
		t.Fatalf("unexpected complete stats: %+v", stats)
	}
	if stats.FCT100 != 2 {
		t.Fatalf("complete FCT100 = %v, want 2", stats.FCT100)
	}
}

func TestOnDataCountsSequenceOnlyOnce(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	name := recordTestSend(t, flow, 1, false)
	payload, err := datapacket.NewDataPacket().SerializeData()
	if err != nil {
		t.Fatalf("SerializeData: %v", err)
	}
	receiveTime := time.Now()

	if !flow.OnData(payload, name, receiveTime) {
		t.Fatal("first valid Data was not accepted")
	}
	if flow.OnData(payload, name, receiveTime.Add(time.Millisecond)) {
		t.Fatal("duplicate Data was accepted")
	}
	if flow.pktsReceivedTotal != 1 || !flow.receivedSeqs[1] {
		t.Fatalf("received state = count %d, seq1 %v", flow.pktsReceivedTotal, flow.receivedSeqs[1])
	}
}

func TestRetransmittedDataFeedsRateSamplesWithoutChangingRTO(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	flow.pktsCalibrated = 5
	flow.baseRTT = 40
	flow.adjustmentPeriod = DT_STATIC_CONTROL_PERIOD
	flow.rto = 777
	name := recordTestSend(t, flow, 1, false)
	if _, ok := flow.RecordInterestSend(name, 1, time.Now(), true); !ok {
		t.Fatal("retransmission send was rejected")
	}
	payload, err := datapacket.NewDataPacket().SerializeData()
	if err != nil {
		t.Fatalf("SerializeData: %v", err)
	}

	if !flow.OnData(payload, name, time.Now().Add(10*time.Millisecond)) {
		t.Fatal("retransmitted Data was not accepted")
	}
	if len(flow.window) != 1 || flow.window[0].RTTValid {
		t.Fatalf("retransmitted Data sample = %+v, want one non-RTT sample", flow.window)
	}
	if flow.rto != 777 {
		t.Fatalf("retransmitted Data changed RTO to %v, want 777", flow.rto)
	}
}

func TestInvalidDTDataLeavesRequestPending(t *testing.T) {
	flow := newDTTestFlow("/pro0app")
	name := recordTestSend(t, flow, 1, false)

	if flow.OnData([]byte("invalid"), name, time.Now()) {
		t.Fatal("invalid Data was accepted")
	}
	if _, pending := flow.pendingSends[name]; !pending {
		t.Fatal("invalid Data cleared the pending logical request")
	}
	if !flow.QueueRetransmission(1) {
		t.Fatal("invalid Data could not be retransmitted")
	}
}

func TestParseConsumerIndex(t *testing.T) {
	tests := []struct {
		name    string
		want    int
		wantErr bool
	}{
		{name: "con0app", want: 0},
		{name: "con1app", want: 1},
		{name: "con24app", want: 24},
		{name: "con1", want: 1},
		{name: "conapp", wantErr: true},
		{name: "con-1app", wantErr: true},
		{name: "core1app", wantErr: true},
		{name: "con1app-extra", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseConsumerIndex(test.name)
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseConsumerIndex(%q) = %d, want error", test.name, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("parseConsumerIndex(%q) = (%d, %v), want (%d, nil)", test.name, got, err, test.want)
			}
		})
	}
}
