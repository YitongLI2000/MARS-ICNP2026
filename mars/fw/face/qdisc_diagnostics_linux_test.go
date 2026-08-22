//go:build linux

package face

import "testing"

func TestParseDiagnosticBool(t *testing.T) {
	tests := map[string]bool{
		"":        false,
		"0":       false,
		"false":   false,
		"garbage": false,
		"1":       true,
		"TRUE":    true,
		" yes ":   true,
		"on":      true,
	}
	for input, expected := range tests {
		if actual := parseDiagnosticBool(input); actual != expected {
			t.Fatalf("parseDiagnosticBool(%q) = %v, want %v", input, actual, expected)
		}
	}
}

func TestNewQdiscSamplerDiagnosticsHonorsDisabledState(t *testing.T) {
	if diagnostics := newQdiscSamplerDiagnostics(false); diagnostics != nil {
		t.Fatal("disabled qdisc diagnostics returned a non-nil collector")
	}
	if diagnostics := newQdiscSamplerDiagnostics(true); diagnostics == nil {
		t.Fatal("enabled qdisc diagnostics returned a nil collector")
	}
}

func TestQdiscSamplerDiagnosticsCountPersistentResources(t *testing.T) {
	diagnostics := newQdiscSamplerDiagnostics(true)
	diagnostics.recordNetlinkSocketOpen()
	diagnostics.recordReadBufferAllocation()
	interval := diagnostics.takeInterval()
	if interval.netlinkSocketOpens != 1 || interval.readBufferAllocations != 1 {
		t.Fatalf(
			"resource counters = (%d socket opens, %d buffers), want (1, 1)",
			interval.netlinkSocketOpens,
			interval.readBufferAllocations,
		)
	}
}

func TestParseUDPSNMPCounters(t *testing.T) {
	fixture := []byte(`Ip: Forwarding DefaultTTL InReceives
Ip: 1 64 10
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors
Udp: 123 4 5 456 6 7 8 9 10
UdpLite: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors
UdpLite: 0 0 0 0 0 0 0
`)

	actual, err := parseUDPSNMPCounters(fixture)
	if err != nil {
		t.Fatalf("parseUDPSNMPCounters returned an error: %v", err)
	}
	expected := udpSNMPCounters{
		inDatagrams:  123,
		inErrors:     5,
		rcvbufErrors: 6,
		sndbufErrors: 7,
	}
	if actual != expected {
		t.Fatalf("parseUDPSNMPCounters() = %+v, want %+v", actual, expected)
	}
}

func TestParseUDPSNMPCountersRejectsMissingFields(t *testing.T) {
	fixture := []byte("Udp: InDatagrams InErrors\nUdp: 10 2\n")
	if _, err := parseUDPSNMPCounters(fixture); err == nil {
		t.Fatal("parseUDPSNMPCounters accepted a UDP section with missing fields")
	}
}

func TestCounterDeltaDoesNotUnderflow(t *testing.T) {
	if actual := counterDelta(12, 5); actual != 7 {
		t.Fatalf("counterDelta(12, 5) = %d, want 7", actual)
	}
	if actual := counterDelta(5, 12); actual != 0 {
		t.Fatalf("counterDelta(5, 12) = %d, want 0", actual)
	}
}
