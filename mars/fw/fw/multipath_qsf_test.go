package fw

import (
	"math"
	"testing"
)

func TestNormalizeDtQueueSignal(t *testing.T) {
	tests := []struct {
		name     string
		raw      float64
		capacity float64
		want     float64
	}{
		{name: "empty", raw: 0, capacity: dtInterestQueueCapacityPackets, want: 0},
		{name: "iq half", raw: 10, capacity: dtInterestQueueCapacityPackets, want: 0.5},
		{name: "iq high", raw: 16, capacity: dtInterestQueueCapacityPackets, want: 0.8},
		{name: "qdisc half", raw: 500, capacity: dtQdiscQueueCapacityPackets, want: 0.5},
		{name: "qdisc high", raw: 800, capacity: dtQdiscQueueCapacityPackets, want: 0.8},
		{name: "clamped", raw: 1200, capacity: dtQdiscQueueCapacityPackets, want: 1.0},
		{name: "invalid capacity", raw: 5, capacity: 0, want: 0},
		{name: "negative raw", raw: -1, capacity: dtInterestQueueCapacityPackets, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeDtQueueSignal(tc.raw, tc.capacity); got != tc.want {
				t.Fatalf("normalizeDtQueueSignal(%v, %v) = %v, want %v", tc.raw, tc.capacity, got, tc.want)
			}
		})
	}
}

func TestConditionDtDataQsfClampsNormalizedRange(t *testing.T) {
	tests := []struct {
		name string
		raw  float64
		want float64
	}{
		{name: "below zero", raw: -0.5, want: 0},
		{name: "in range", raw: 0.6, want: 0.6},
		{name: "above one", raw: 1.4, want: 1.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := conditionDtDataQsf("qdisc", tc.raw); got != tc.want {
				t.Fatalf("conditionDtDataQsf(%q, %v) = %v, want %v", "qdisc", tc.raw, got, tc.want)
			}
		})
	}
}

func TestDtFaceBandwidthCapLimit(t *testing.T) {
	tests := []struct {
		name         string
		estimatedPps float64
		wantLimitPps float64
	}{
		{
			name:         "invalid estimate disables cap",
			estimatedPps: 0,
			wantLimitPps: 0,
		},
		{
			name:         "early bootstrap estimate retains initial probe floor",
			estimatedPps: dtFaceInitRate / 2,
			wantLimitPps: dtFaceInitRate,
		},
		{
			name:         "estimate just below initial rate is still capped",
			estimatedPps: dtFaceInitRate - 0.01,
			wantLimitPps: (dtFaceInitRate - 0.01) * dtRateControlParams.MaxBWSafetyRatio,
		},
		{
			name:         "estimate above initial rate uses bandwidth safety ratio",
			estimatedPps: dtFaceInitRate * 2,
			wantLimitPps: dtFaceInitRate * 2 * dtRateControlParams.MaxBWSafetyRatio,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := dtFaceBandwidthCapLimit(tc.estimatedPps)
			if math.Abs(got-tc.wantLimitPps) > 1e-9 {
				t.Fatalf("dtFaceBandwidthCapLimit(%v) = %v, want %v", tc.estimatedPps, got, tc.wantLimitPps)
			}
		})
	}
}

func TestDtFaceBandwidthCapReadyAfterBootstrapBelowInitialRate(t *testing.T) {
	estimatedPps := dtFaceInitRate - 0.01
	if dtFaceBandwidthCapReady(false, estimatedPps) {
		t.Fatal("bandwidth cap became ready before bootstrap completed")
	}
	if !dtFaceBandwidthCapReady(true, estimatedPps) {
		t.Fatal("bandwidth cap did not become ready after bootstrap below the initial rate")
	}
}
