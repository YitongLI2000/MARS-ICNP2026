//go:build linux

package face

import (
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestQdiscBacklogPollIntervalIsTwoMilliseconds(t *testing.T) {
	if qdiscBacklogPollInterval != 2*time.Millisecond {
		t.Fatalf("qdiscBacklogPollInterval = %s, want 2ms", qdiscBacklogPollInterval)
	}
}

func TestParseQdiscBacklogFromStats2(t *testing.T) {
	queue := make([]byte, 8)
	native.PutUint32(queue[0:4], 17)
	native.PutUint32(queue[4:8], 24680)
	stats2 := testRtAttr(tcaStatsQueue, queue)
	attrs := append(
		testRtAttr(tcaKind, append([]byte("netem"), 0)),
		testRtAttr(tcaStats2, stats2)...,
	)

	backlogBytes, backlogPackets := parseQdiscBacklogFromAttrs(attrs)
	if backlogBytes != 24680 || backlogPackets != 17 {
		t.Fatalf(
			"parseQdiscBacklogFromAttrs() = (%d, %d), want (24680, 17)",
			backlogBytes,
			backlogPackets,
		)
	}
	if kind := parseQdiscKind(attrs); kind != "netem" {
		t.Fatalf("parseQdiscKind() = %q, want netem", kind)
	}
	if !qdiscKindEquals(attrs, "netem") {
		t.Fatal("qdiscKindEquals() did not match netem")
	}
	if qdiscKindEquals(attrs, "htb") {
		t.Fatal("qdiscKindEquals() matched the wrong qdisc kind")
	}
}

func TestQdiscInterfaceAuditDetectsMatchingRootAndChild(t *testing.T) {
	audit := &qdiscInterfaceAudit{}
	audit.add(qdiscLayerAudit{
		kind: "htb", handle: 0x00050000, parent: tcHandleRoot,
		bytes: 14200, packets: 10,
	})
	audit.add(qdiscLayerAudit{
		kind: "netem", handle: 0x000a0000, parent: 0x00050001,
		bytes: 14200, packets: 10,
	})

	if !audit.possibleDoubleCount() {
		t.Fatal("matching nonzero root and child backlogs were not flagged")
	}
	if audit.maxLayerPkts != 10 || audit.maxLayerBytes != 14200 {
		t.Fatalf(
			"max layer = (%d bytes, %d packets), want (14200, 10)",
			audit.maxLayerBytes,
			audit.maxLayerPkts,
		)
	}
	if audit.selected.counters.packets != 10 || audit.selected.counters.bytes != 14200 {
		t.Fatalf(
			"selected backlog = (%d bytes, %d packets), want (14200, 10)",
			audit.selected.counters.bytes,
			audit.selected.counters.packets,
		)
	}
	if audit.selected.priority != qdiscSelectionNonRootNetem || audit.selected.entries != 1 {
		t.Fatalf(
			"selected priority/entries = (%d, %d), want (%d, 1)",
			audit.selected.priority,
			audit.selected.entries,
			qdiscSelectionNonRootNetem,
		)
	}
	selectedKinds, selectedHandles := audit.describeSelectedLayers()
	if selectedKinds != "netem" || selectedHandles != "a:0" {
		t.Fatalf(
			"selected layers = (%q, %q), want (netem, a:0)",
			selectedKinds,
			selectedHandles,
		)
	}
	description := audit.describeLayers()
	for _, expected := range []string{"htb@5:0", "parent=root", "netem@a:0", "parent=5:1"} {
		if !strings.Contains(description, expected) {
			t.Fatalf("layer description %q does not contain %q", description, expected)
		}
	}
}

func TestQdiscSelectionUsesNetemEvenWhenItsSnapshotIsZero(t *testing.T) {
	selection := qdiscBacklogSelection{}
	selection.consider(
		qdiscBacklogCounters{bytes: 14200, packets: 10},
		qdiscSelectionPriority(tcHandleRoot, false),
	)
	selection.consider(
		qdiscBacklogCounters{},
		qdiscSelectionPriority(0x00050001, true),
	)

	if selection.priority != qdiscSelectionNonRootNetem {
		t.Fatalf(
			"selected priority = %d, want %d",
			selection.priority,
			qdiscSelectionNonRootNetem,
		)
	}
	if selection.counters != (qdiscBacklogCounters{}) {
		t.Fatalf("selected stale root backlog: %+v", selection.counters)
	}
}

func TestQdiscSelectionFallsBackToNonRootThenRoot(t *testing.T) {
	selection := qdiscBacklogSelection{}
	selection.consider(
		qdiscBacklogCounters{bytes: 1000, packets: 1},
		qdiscSelectionPriority(tcHandleRoot, false),
	)
	selection.consider(
		qdiscBacklogCounters{bytes: 2000, packets: 2},
		qdiscSelectionPriority(0x00050001, false),
	)

	if selection.priority != qdiscSelectionNonRoot {
		t.Fatalf(
			"selected priority = %d, want %d",
			selection.priority,
			qdiscSelectionNonRoot,
		)
	}
	if selection.counters != (qdiscBacklogCounters{bytes: 2000, packets: 2}) {
		t.Fatalf("selected backlog = %+v, want non-root backlog", selection.counters)
	}
}

func TestQdiscInterfaceAuditRejectsDifferentLayerBacklogs(t *testing.T) {
	audit := &qdiscInterfaceAudit{}
	audit.add(qdiscLayerAudit{parent: tcHandleRoot, bytes: 1000, packets: 1})
	audit.add(qdiscLayerAudit{parent: 0x00050001, bytes: 2000, packets: 2})
	if audit.possibleDoubleCount() {
		t.Fatal("different root and child backlogs were flagged as a duplicate")
	}
}

func TestQdiscNetlinkReaderReusesSocketAndBuffers(t *testing.T) {
	reader, err := newQdiscNetlinkReader()
	if err != nil {
		t.Skipf("route-netlink reader is unavailable: %v", err)
	}
	defer reader.close()

	fd := reader.fd
	request := &reader.request[0]
	readBuffer := &reader.readBuffer[0]
	initialSeq := reader.seq
	for iteration := 0; iteration < 2; iteration++ {
		err = reader.dump(func(syscall.NetlinkMessage) {})
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("route-netlink dump is not permitted: %v", err)
		}
		if err != nil {
			t.Fatalf("route-netlink dump %d failed: %v", iteration+1, err)
		}
	}

	if reader.fd != fd {
		t.Fatalf("netlink fd changed from %d to %d", fd, reader.fd)
	}
	if &reader.request[0] != request {
		t.Fatal("netlink request buffer was replaced")
	}
	if &reader.readBuffer[0] != readBuffer {
		t.Fatal("netlink read buffer was replaced")
	}
	if reader.seq != initialSeq+2 {
		t.Fatalf("netlink sequence advanced to %d, want %d", reader.seq, initialSeq+2)
	}
}

func testRtAttr(attrType uint16, value []byte) []byte {
	attrLen := 4 + len(value)
	encoded := make([]byte, align4(attrLen))
	native.PutUint16(encoded[0:2], uint16(attrLen))
	native.PutUint16(encoded[2:4], attrType)
	copy(encoded[4:], value)
	return encoded
}
