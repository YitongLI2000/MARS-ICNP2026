package main

import (
	"sort"
	"time"

	"github.com/named-data/ndnd/std/log"
)

const shadowLossScanInterval = 20 * time.Millisecond

const (
	shadowFastRepairAgeFloor          = 350 * time.Millisecond
	shadowFastRepairLaterACKThreshold = 8
)

type shadowLossConfiguration struct {
	AgeFloor          time.Duration
	LaterACKThreshold int
}

var shadowLossConfigurations = []shadowLossConfiguration{
	{AgeFloor: 250 * time.Millisecond, LaterACKThreshold: 8},
	{AgeFloor: 250 * time.Millisecond, LaterACKThreshold: 16},
	{AgeFloor: 250 * time.Millisecond, LaterACKThreshold: 32},
	{AgeFloor: 350 * time.Millisecond, LaterACKThreshold: 8},
	{AgeFloor: 350 * time.Millisecond, LaterACKThreshold: 16},
	{AgeFloor: 350 * time.Millisecond, LaterACKThreshold: 32},
	{AgeFloor: 500 * time.Millisecond, LaterACKThreshold: 8},
	{AgeFloor: 500 * time.Millisecond, LaterACKThreshold: 16},
	{AgeFloor: 500 * time.Millisecond, LaterACKThreshold: 32},
}

type shadowCandidateStatus uint8

const (
	shadowCandidateNone shadowCandidateStatus = iota
	shadowCandidateActive
	shadowCandidateResolvedBeforeRTO
	shadowCandidateReachedRTO
)

type shadowPendingSequence struct {
	firstSentAt                   time.Time
	laterACKs                     int
	inRecovery                    bool
	generation                    uint64
	fastRepairTriggeredGeneration uint64
	fastRepairQueuedGeneration    uint64
	fastRepairSent                bool
}

type shadowFastRepairCandidate struct {
	Sequence   int
	Generation uint64
	Age        time.Duration
	LaterACKs  int
}

type shadowFastRepairQueueOutcome uint8

const (
	shadowFastRepairQueueStale shadowFastRepairQueueOutcome = iota
	shadowFastRepairQueueQueued
	shadowFastRepairQueueAlreadyQueued
	shadowFastRepairQueueAlreadyTriggered
)

func (outcome shadowFastRepairQueueOutcome) String() string {
	switch outcome {
	case shadowFastRepairQueueQueued:
		return "queued"
	case shadowFastRepairQueueAlreadyQueued:
		return "already-queued"
	case shadowFastRepairQueueAlreadyTriggered:
		return "already-triggered"
	default:
		return "stale"
	}
}

type shadowLossThresholdState struct {
	configuration       shadowLossConfiguration
	status              map[int]shadowCandidateStatus
	suspectedAt         map[int]time.Time
	shadowSuspects      int
	resolvedBeforeRTO   int
	reachedRTO          int
	leadToRTOTotal      time.Duration
	suspectInflightTime time.Duration
}

type shadowLossDetector struct {
	pending                     map[int]*shadowPendingSequence
	states                      []shadowLossThresholdState
	lastScan                    time.Time
	firstAttemptRTOs            int
	fastRepairTriggers          int
	fastRepairQueued            int
	fastRepairAlreadyQueued     int
	fastRepairAlreadyTriggered  int
	fastRepairStale             int
	fastRepairSent              int
	fastRepairResolved          int
	fastRepairResolvedAfterSend int
}

type ShadowLossSummary struct {
	AgeFloor             time.Duration
	LaterACKThreshold    int
	ShadowSuspects       int
	ResolvedBeforeRTO    int
	ReachedRTO           int
	ActiveAtEnd          int
	LeadToRTOTotal       time.Duration
	AvgLeadToRTO         time.Duration
	SuspectInflightTotal time.Duration
	SuspectInflightAvg   time.Duration
	FirstAttemptRTOs     int
}

type ShadowFastRepairSummary struct {
	AgeFloor          time.Duration
	LaterACKThreshold int
	Triggers          int
	Queued            int
	AlreadyQueued     int
	AlreadyTriggered  int
	Stale             int
	Sent              int
	Resolved          int
	ResolvedAfterSend int
}

func (d *shadowLossDetector) reset() {
	d.pending = make(map[int]*shadowPendingSequence)
	d.states = make([]shadowLossThresholdState, len(shadowLossConfigurations))
	for i, configuration := range shadowLossConfigurations {
		d.states[i] = shadowLossThresholdState{
			configuration: configuration,
			status:        make(map[int]shadowCandidateStatus),
			suspectedAt:   make(map[int]time.Time),
		}
	}
	d.lastScan = time.Time{}
	d.firstAttemptRTOs = 0
	d.fastRepairTriggers = 0
	d.fastRepairQueued = 0
	d.fastRepairAlreadyQueued = 0
	d.fastRepairAlreadyTriggered = 0
	d.fastRepairStale = 0
	d.fastRepairSent = 0
	d.fastRepairResolved = 0
	d.fastRepairResolvedAfterSend = 0
}

func (d *shadowLossDetector) ensureInitialized() {
	if d.pending == nil || len(d.states) != len(shadowLossConfigurations) {
		d.reset()
	}
}

func (d *shadowLossDetector) recordFreshSendLocked(sequence int, sentAt time.Time) {
	if sequence <= 0 {
		return
	}
	d.ensureInitialized()
	if _, exists := d.pending[sequence]; exists {
		return
	}
	d.pending[sequence] = &shadowPendingSequence{firstSentAt: sentAt}
}

func (d *shadowLossDetector) recordRecoverySendLocked(sequence int) {
	d.ensureInitialized()
	if pending, exists := d.pending[sequence]; exists {
		pending.inRecovery = true
	}
}

func (d *shadowLossDetector) recordAttemptLocked(sequence int, generation uint64) {
	if sequence <= 0 || generation == 0 {
		return
	}
	d.ensureInitialized()
	if pending, exists := d.pending[sequence]; exists {
		pending.generation = generation
	}
}

func (d *shadowLossDetector) recordFastRepairSendLocked(sequence int) {
	d.ensureInitialized()
	d.fastRepairSent++
	if pending, exists := d.pending[sequence]; exists {
		pending.fastRepairSent = true
	}
}

func nonNegativeDuration(end time.Time, start time.Time) time.Duration {
	duration := end.Sub(start)
	if duration < 0 {
		return 0
	}
	return duration
}

func (d *shadowLossDetector) finishCandidateLocked(
	state *shadowLossThresholdState,
	sequence int,
	status shadowCandidateStatus,
	finishedAt time.Time,
) {
	suspectedAt, active := state.suspectedAt[sequence]
	if !active || state.status[sequence] != shadowCandidateActive {
		return
	}
	duration := nonNegativeDuration(finishedAt, suspectedAt)
	state.suspectInflightTime += duration
	delete(state.suspectedAt, sequence)
	state.status[sequence] = status
	switch status {
	case shadowCandidateResolvedBeforeRTO:
		state.resolvedBeforeRTO++
	case shadowCandidateReachedRTO:
		state.reachedRTO++
		state.leadToRTOTotal += duration
	}
}

func (d *shadowLossDetector) recordDataLocked(sequence int, receivedAt time.Time) {
	d.ensureInitialized()
	if pending, exists := d.pending[sequence]; exists {
		if pending.fastRepairQueuedGeneration != 0 {
			d.fastRepairResolved++
			if pending.fastRepairSent {
				d.fastRepairResolvedAfterSend++
			}
		}
		for i := range d.states {
			d.finishCandidateLocked(
				&d.states[i], sequence, shadowCandidateResolvedBeforeRTO, receivedAt,
			)
		}
		delete(d.pending, sequence)
	}

	for pendingSequence, pending := range d.pending {
		if pendingSequence < sequence && !pending.inRecovery {
			pending.laterACKs++
		}
	}
}

func (d *shadowLossDetector) recordRTOLocked(sequence int, expiredAt time.Time) {
	d.ensureInitialized()
	if pending, exists := d.pending[sequence]; exists {
		if !pending.inRecovery {
			d.firstAttemptRTOs++
		}
		pending.inRecovery = true
	}
	for i := range d.states {
		d.finishCandidateLocked(
			&d.states[i], sequence, shadowCandidateReachedRTO, expiredAt,
		)
	}
}

func (d *shadowLossDetector) scanLocked(now time.Time) {
	d.scanPendingLocked(now, false)
}

func (d *shadowLossDetector) scanPendingLocked(
	now time.Time,
	collectFastRepair bool,
) []shadowFastRepairCandidate {
	d.ensureInitialized()
	var candidates []shadowFastRepairCandidate
	for sequence, pending := range d.pending {
		if pending.inRecovery || pending.firstSentAt.IsZero() {
			continue
		}
		age := nonNegativeDuration(now, pending.firstSentAt)
		for i := range d.states {
			state := &d.states[i]
			if state.status[sequence] != shadowCandidateNone ||
				age < state.configuration.AgeFloor ||
				pending.laterACKs < state.configuration.LaterACKThreshold {
				continue
			}
			state.status[sequence] = shadowCandidateActive
			state.suspectedAt[sequence] = now
			state.shadowSuspects++
		}
		if !collectFastRepair || pending.generation == 0 {
			continue
		}
		if age < shadowFastRepairAgeFloor ||
			pending.laterACKs < shadowFastRepairLaterACKThreshold ||
			pending.fastRepairTriggeredGeneration == pending.generation {
			continue
		}
		pending.fastRepairTriggeredGeneration = pending.generation
		d.fastRepairTriggers++
		candidates = append(candidates, shadowFastRepairCandidate{
			Sequence:   sequence,
			Generation: pending.generation,
			Age:        age,
			LaterACKs:  pending.laterACKs,
		})
	}
	if len(candidates) > 1 {
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Age == candidates[j].Age {
				return candidates[i].Sequence < candidates[j].Sequence
			}
			return candidates[i].Age > candidates[j].Age
		})
	}
	return candidates
}

func (d *shadowLossDetector) scanIfDueLocked(now time.Time) []shadowFastRepairCandidate {
	d.ensureInitialized()
	if !d.lastScan.IsZero() && now.Sub(d.lastScan) < shadowLossScanInterval {
		return nil
	}
	d.lastScan = now
	return d.scanPendingLocked(now, true)
}

func (d *shadowLossDetector) recordFastRepairQueueOutcomeLocked(
	candidate shadowFastRepairCandidate,
	outcome shadowFastRepairQueueOutcome,
) {
	d.ensureInitialized()
	switch outcome {
	case shadowFastRepairQueueQueued:
		d.fastRepairQueued++
		if pending, exists := d.pending[candidate.Sequence]; exists &&
			pending.generation == candidate.Generation {
			pending.fastRepairQueuedGeneration = candidate.Generation
		}
	case shadowFastRepairQueueAlreadyQueued:
		d.fastRepairAlreadyQueued++
	case shadowFastRepairQueueAlreadyTriggered:
		d.fastRepairAlreadyTriggered++
	default:
		d.fastRepairStale++
	}
}

func averageDuration(total time.Duration, count int) time.Duration {
	if count <= 0 {
		return 0
	}
	return time.Duration(int64(total) / int64(count))
}

func (d *shadowLossDetector) summariesLocked(now time.Time) []ShadowLossSummary {
	d.ensureInitialized()
	summaries := make([]ShadowLossSummary, 0, len(d.states))
	for i := range d.states {
		state := &d.states[i]
		inflightTotal := state.suspectInflightTime
		for _, suspectedAt := range state.suspectedAt {
			inflightTotal += nonNegativeDuration(now, suspectedAt)
		}
		summaries = append(summaries, ShadowLossSummary{
			AgeFloor:             state.configuration.AgeFloor,
			LaterACKThreshold:    state.configuration.LaterACKThreshold,
			ShadowSuspects:       state.shadowSuspects,
			ResolvedBeforeRTO:    state.resolvedBeforeRTO,
			ReachedRTO:           state.reachedRTO,
			ActiveAtEnd:          len(state.suspectedAt),
			LeadToRTOTotal:       state.leadToRTOTotal,
			AvgLeadToRTO:         averageDuration(state.leadToRTOTotal, state.reachedRTO),
			SuspectInflightTotal: inflightTotal,
			SuspectInflightAvg:   averageDuration(inflightTotal, state.shadowSuspects),
			FirstAttemptRTOs:     d.firstAttemptRTOs,
		})
	}
	return summaries
}

func (d *shadowLossDetector) fastRepairSummaryLocked() ShadowFastRepairSummary {
	d.ensureInitialized()
	return ShadowFastRepairSummary{
		AgeFloor:          shadowFastRepairAgeFloor,
		LaterACKThreshold: shadowFastRepairLaterACKThreshold,
		Triggers:          d.fastRepairTriggers,
		Queued:            d.fastRepairQueued,
		AlreadyQueued:     d.fastRepairAlreadyQueued,
		AlreadyTriggered:  d.fastRepairAlreadyTriggered,
		Stale:             d.fastRepairStale,
		Sent:              d.fastRepairSent,
		Resolved:          d.fastRepairResolved,
		ResolvedAfterSend: d.fastRepairResolvedAfterSend,
	}
}

func (f *FlowContext) runShadowLossScan(
	now time.Time,
	controller *RetransmissionController,
) {
	f.mu.Lock()
	if f.Mode != ModeDT || f.isFinished {
		f.mu.Unlock()
		return
	}
	candidates := f.shadowLoss.scanIfDueLocked(now)
	f.mu.Unlock()

	if controller == nil {
		return
	}
	for _, candidate := range candidates {
		name := f.interestNameForSeq(candidate.Sequence)
		outcome := controller.QueueShadowFastRepairIfCurrent(name, candidate, f)
		log.Debug(consumerTag, "Shadow Fast Repair Trigger",
			"name", name,
			"generation", candidate.Generation,
			"ageMs", durationMilliseconds(candidate.Age),
			"laterACKs", candidate.LaterACKs,
			"outcome", outcome.String())
	}
}

func (f *FlowContext) RecordShadowRTO(sequence int, expiredAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Mode != ModeDT {
		return
	}
	f.shadowLoss.recordRTOLocked(sequence, expiredAt)
}

func (f *FlowContext) installShadowAttemptAndClearQueued(
	sequence int,
	generation uint64,
	isFastRepair bool,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Mode == ModeDT {
		f.shadowLoss.recordAttemptLocked(sequence, generation)
		if isFastRepair {
			f.shadowLoss.recordFastRepairSendLocked(sequence)
		}
	}
	f.clearQueuedRetransmissionLocked(sequence)
}

func (f *FlowContext) GetShadowLossSummaries(now time.Time) []ShadowLossSummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shadowLoss.summariesLocked(now)
}

func (f *FlowContext) GetShadowFastRepairSummary() ShadowFastRepairSummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shadowLoss.fastRepairSummaryLocked()
}

func durationMilliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}
