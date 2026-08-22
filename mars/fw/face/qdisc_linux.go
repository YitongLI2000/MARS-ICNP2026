//go:build linux

package face

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/named-data/ndnd/fw/core"
)

const (
	qdiscBacklogPollInterval   = 2 * time.Millisecond
	qdiscNetlinkReadBufferSize = 1 << 20
	qdiscDataQsfAggregation    = "prefer-non-root-netem"

	tcmsgLen     = 20
	tcHandleRoot = ^uint32(0)

	tcaKind   = 1
	tcaStats  = 3
	tcaStats2 = 7

	tcaStatsQueue = 3

	qdiscSelectionRoot         uint8 = 1
	qdiscSelectionNonRoot      uint8 = 2
	qdiscSelectionNonRootNetem uint8 = 3
)

var (
	qdiscBacklogSamplesByIfIndex sync.Map
	qdiscBacklogSamplerOnce      sync.Once
)

type qdiscBacklogSample struct {
	backlog *transportLinkBacklog
	ifName  string
	ifIndex int
}

type qdiscBacklogCounters struct {
	bytes   uint64
	packets uint64
}

type qdiscBacklogSelection struct {
	counters qdiscBacklogCounters
	priority uint8
	entries  uint32
}

func (selection *qdiscBacklogSelection) consider(counters qdiscBacklogCounters, priority uint8) {
	if priority > selection.priority {
		selection.counters = counters
		selection.priority = priority
		selection.entries = 1
		return
	}
	if priority == selection.priority {
		selection.counters.bytes += counters.bytes
		selection.counters.packets += counters.packets
		selection.entries++
	}
}

type qdiscNetlinkReader struct {
	fd         int
	seq        uint32
	request    []byte
	readBuffer []byte
	backlogs   map[int]qdiscBacklogSelection
}

type qdiscLayerAudit struct {
	kind    string
	handle  uint32
	parent  uint32
	bytes   uint64
	packets uint64
}

type qdiscInterfaceAudit struct {
	layers         []qdiscLayerAudit
	selected       qdiscBacklogSelection
	rootBytes      uint64
	rootPackets    uint64
	nonRootBytes   uint64
	nonRootPackets uint64
	maxLayerBytes  uint64
	maxLayerPkts   uint64
}

func maybeStartQdiscBacklogSampler(localIP net.IP, tag any) *transportLinkBacklog {
	if localIP == nil || localIP.IsUnspecified() {
		return nil
	}

	iface, err := interfaceByLocalIP(localIP)
	if err != nil {
		core.Log.Warn(nil, "Unable to resolve qdisc interface for UDP face",
			"localIP", localIP.String(),
			"transport", tag,
			"err", err)
		return nil
	}

	sample := &qdiscBacklogSample{
		backlog: &transportLinkBacklog{},
		ifName:  iface.Name,
		ifIndex: iface.Index,
	}
	actual, loaded := qdiscBacklogSamplesByIfIndex.LoadOrStore(iface.Index, sample)
	state := actual.(*qdiscBacklogSample)
	if !loaded {
		core.Log.Info(nil, "Registered qdisc backlog interface",
			"transport", tag,
			"interface", state.ifName,
			"ifindex", state.ifIndex)
	}
	qdiscBacklogSamplerOnce.Do(func() {
		core.Log.Info(nil, "Started global qdisc backlog sampler",
			"period", qdiscBacklogPollInterval,
			"persistentNetlink", true,
			"readBufferBytes", qdiscNetlinkReadBufferSize,
			"dataQsfAggregation", qdiscDataQsfAggregation,
			"hierarchyAudit", qdiscHierarchyAuditEnabled)
		if qdiscSamplerDiagnostics != nil {
			baseline := captureQdiscResourceSnapshot()
			go runQdiscSamplerDiagnosticReporter(qdiscSamplerDiagnostics, baseline)
		}
		go runQdiscBacklogSampler()
	})
	return state.backlog
}

func interfaceByLocalIP(ip net.IP) (*net.Interface, error) {
	target4 := ip.To4()
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var addrIP net.IP
			switch typed := addr.(type) {
			case *net.IPNet:
				addrIP = typed.IP
			case *net.IPAddr:
				addrIP = typed.IP
			default:
				continue
			}
			if target4 != nil {
				if addrIP.To4() != nil && addrIP.To4().Equal(target4) {
					return &iface, nil
				}
			} else if addrIP.Equal(ip) {
				return &iface, nil
			}
		}
	}
	return nil, errors.New("no interface owns local IP")
}

func runQdiscBacklogSampler() {
	ticker := time.NewTicker(qdiscBacklogPollInterval)
	defer ticker.Stop()

	var lastWarn time.Time
	var reader *qdiscNetlinkReader
	nextAudit := time.Now().Add(qdiscHierarchyAuditInterval)
	defer func() {
		if reader != nil {
			reader.close()
		}
	}()

	for !core.ShouldQuit {
		var sampleStarted time.Time
		if qdiscSamplerDiagnostics != nil {
			sampleStarted = time.Now()
		}

		now := time.Now()
		var audit map[int]*qdiscInterfaceAudit
		if qdiscHierarchyAuditEnabled && !now.Before(nextAudit) {
			audit = make(map[int]*qdiscInterfaceAudit)
			nextAudit = now.Add(qdiscHierarchyAuditInterval)
		}

		var err error
		if reader == nil {
			reader, err = newQdiscNetlinkReader()
		}
		if err == nil {
			err = sampleAllRegisteredQdiscBacklogs(reader, audit)
		}
		if qdiscSamplerDiagnostics != nil {
			qdiscSamplerDiagnostics.recordSample(time.Since(sampleStarted), err)
		}
		if err != nil {
			if reader != nil {
				reader.close()
				reader = nil
			}
			if time.Since(lastWarn) >= time.Second {
				lastWarn = time.Now()
				core.Log.Warn(nil, "Unable to sample qdisc backlog",
					"err", err)
			}
		} else if audit != nil {
			logQdiscHierarchyAudit(audit)
		}
		<-ticker.C
	}
}

func sampleAllRegisteredQdiscBacklogs(
	reader *qdiscNetlinkReader,
	audit map[int]*qdiscInterfaceAudit,
) error {
	clear(reader.backlogs)
	err := reader.dump(func(msg syscall.NetlinkMessage) {
		if msg.Header.Type != syscall.RTM_NEWQDISC {
			return
		}
		if len(msg.Data) < tcmsgLen {
			return
		}
		ifIndex := int(native.Uint32(msg.Data[4:8]))
		if _, registered := qdiscBacklogSamplesByIfIndex.Load(ifIndex); !registered {
			return
		}
		attrData := msg.Data[align4(tcmsgLen):]
		backlogBytes, backlogPackets := parseQdiscBacklogFromAttrs(attrData)
		parent := native.Uint32(msg.Data[12:16])
		priority := qdiscSelectionPriority(parent, qdiscKindEquals(attrData, "netem"))
		selection := reader.backlogs[ifIndex]
		selection.consider(qdiscBacklogCounters{
			bytes:   backlogBytes,
			packets: backlogPackets,
		}, priority)
		reader.backlogs[ifIndex] = selection

		if audit != nil {
			entry := audit[ifIndex]
			if entry == nil {
				entry = &qdiscInterfaceAudit{}
				audit[ifIndex] = entry
			}
			entry.add(qdiscLayerAudit{
				kind:    parseQdiscKind(attrData),
				handle:  native.Uint32(msg.Data[8:12]),
				parent:  parent,
				bytes:   backlogBytes,
				packets: backlogPackets,
			})
		}
	})
	if err != nil {
		return err
	}

	qdiscBacklogSamplesByIfIndex.Range(func(_, value any) bool {
		sample := value.(*qdiscBacklogSample)
		counters := reader.backlogs[sample.ifIndex].counters
		sample.backlog.bytes.Store(counters.bytes)
		sample.backlog.packets.Store(counters.packets)
		return true
	})
	return nil
}

func parseQdiscBacklogFromAttrs(attrData []byte) (uint64, uint64) {
	if stats2, ok := findRtAttr(attrData, tcaStats2); ok {
		if queue, ok := findRtAttr(stats2, tcaStatsQueue); ok && len(queue) >= 8 {
			return uint64(native.Uint32(queue[4:8])), uint64(native.Uint32(queue[0:4]))
		}
	}
	if stats, ok := findRtAttr(attrData, tcaStats); ok && len(stats) >= 36 {
		return uint64(native.Uint32(stats[32:36])), uint64(native.Uint32(stats[28:32]))
	}
	return 0, 0
}

func parseQdiscKind(attrData []byte) string {
	kind, ok := findRtAttr(attrData, tcaKind)
	if !ok {
		return "unknown"
	}
	if terminator := bytes.IndexByte(kind, 0); terminator >= 0 {
		kind = kind[:terminator]
	}
	if len(kind) == 0 {
		return "unknown"
	}
	return string(kind)
}

func qdiscKindEquals(attrData []byte, expected string) bool {
	kind, ok := findRtAttr(attrData, tcaKind)
	if !ok {
		return false
	}
	if terminator := bytes.IndexByte(kind, 0); terminator >= 0 {
		kind = kind[:terminator]
	}
	if len(kind) != len(expected) {
		return false
	}
	for index := range kind {
		if kind[index] != expected[index] {
			return false
		}
	}
	return true
}

func qdiscSelectionPriority(parent uint32, isNetem bool) uint8 {
	if parent == tcHandleRoot {
		return qdiscSelectionRoot
	}
	if isNetem {
		return qdiscSelectionNonRootNetem
	}
	return qdiscSelectionNonRoot
}

func newQdiscNetlinkReader() (*qdiscNetlinkReader, error) {
	fd, err := syscall.Socket(syscall.AF_NETLINK, syscall.SOCK_RAW|syscall.SOCK_CLOEXEC, syscall.NETLINK_ROUTE)
	if err != nil {
		return nil, err
	}

	if err := syscall.Bind(fd, &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}); err != nil {
		syscall.Close(fd)
		return nil, err
	}

	reader := &qdiscNetlinkReader{
		fd:         fd,
		seq:        uint32(time.Now().UnixNano()),
		request:    make([]byte, syscall.NLMSG_HDRLEN+tcmsgLen),
		readBuffer: make([]byte, qdiscNetlinkReadBufferSize),
		backlogs:   make(map[int]qdiscBacklogSelection),
	}
	native.PutUint32(reader.request[0:4], uint32(len(reader.request)))
	native.PutUint16(reader.request[4:6], syscall.RTM_GETQDISC)
	native.PutUint16(reader.request[6:8], syscall.NLM_F_REQUEST|syscall.NLM_F_DUMP)
	reader.request[syscall.NLMSG_HDRLEN] = syscall.AF_UNSPEC

	if qdiscSamplerDiagnostics != nil {
		qdiscSamplerDiagnostics.recordReadBufferAllocation()
		qdiscSamplerDiagnostics.recordNetlinkSocketOpen()
	}
	return reader, nil
}

func (reader *qdiscNetlinkReader) close() {
	if reader.fd >= 0 {
		_ = syscall.Close(reader.fd)
		reader.fd = -1
	}
}

func (reader *qdiscNetlinkReader) dump(visit func(syscall.NetlinkMessage)) error {
	reader.seq++
	if reader.seq == 0 {
		reader.seq++
	}
	native.PutUint32(reader.request[8:12], reader.seq)

	if err := syscall.Sendto(
		reader.fd,
		reader.request,
		0,
		&syscall.SockaddrNetlink{Family: syscall.AF_NETLINK},
	); err != nil {
		return err
	}

	for {
		n, _, err := syscall.Recvfrom(reader.fd, reader.readBuffer, 0)
		if err != nil {
			return err
		}
		msgs, err := syscall.ParseNetlinkMessage(reader.readBuffer[:n])
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			if msg.Header.Seq != reader.seq {
				continue
			}
			switch msg.Header.Type {
			case syscall.NLMSG_DONE:
				return nil
			case syscall.NLMSG_ERROR:
				if len(msg.Data) >= 4 {
					code := int32(native.Uint32(msg.Data[:4]))
					if code == 0 {
						return nil
					}
					return syscall.Errno(-code)
				}
				return errors.New("netlink qdisc error")
			default:
				visit(msg)
			}
		}
	}
}

func findRtAttr(b []byte, wanted uint16) ([]byte, bool) {
	for len(b) >= 4 {
		attrLen := int(native.Uint16(b[0:2]))
		attrType := native.Uint16(b[2:4])
		if attrLen < 4 || attrLen > len(b) {
			break
		}
		if attrType == wanted {
			return b[4:attrLen], true
		}
		next := align4(attrLen)
		if next > len(b) {
			break
		}
		b = b[next:]
	}
	return nil, false
}

func (audit *qdiscInterfaceAudit) add(layer qdiscLayerAudit) {
	audit.layers = append(audit.layers, layer)
	audit.selected.consider(qdiscBacklogCounters{
		bytes:   layer.bytes,
		packets: layer.packets,
	}, qdiscSelectionPriority(layer.parent, layer.kind == "netem"))
	if layer.parent == tcHandleRoot {
		audit.rootBytes += layer.bytes
		audit.rootPackets += layer.packets
	} else {
		audit.nonRootBytes += layer.bytes
		audit.nonRootPackets += layer.packets
	}
	if layer.bytes > audit.maxLayerBytes {
		audit.maxLayerBytes = layer.bytes
	}
	if layer.packets > audit.maxLayerPkts {
		audit.maxLayerPkts = layer.packets
	}
}

func (audit *qdiscInterfaceAudit) describeSelectedLayers() (string, string) {
	kinds := make([]string, 0, audit.selected.entries)
	handles := make([]string, 0, audit.selected.entries)
	for _, layer := range audit.layers {
		priority := qdiscSelectionPriority(layer.parent, layer.kind == "netem")
		if priority != audit.selected.priority {
			continue
		}
		kinds = append(kinds, layer.kind)
		handles = append(handles, formatTcHandle(layer.handle))
	}
	return strings.Join(kinds, ","), strings.Join(handles, ",")
}

func (audit *qdiscInterfaceAudit) possibleDoubleCount() bool {
	hasBacklog := audit.rootBytes != 0 || audit.rootPackets != 0
	return hasBacklog &&
		audit.rootBytes == audit.nonRootBytes &&
		audit.rootPackets == audit.nonRootPackets
}

func (audit *qdiscInterfaceAudit) describeLayers() string {
	layers := make([]string, 0, len(audit.layers))
	for _, layer := range audit.layers {
		layers = append(layers, fmt.Sprintf(
			"%s@%s,parent=%s,bytes=%d,packets=%d",
			layer.kind,
			formatTcHandle(layer.handle),
			formatTcHandle(layer.parent),
			layer.bytes,
			layer.packets,
		))
	}
	return strings.Join(layers, ";")
}

func logQdiscHierarchyAudit(audits map[int]*qdiscInterfaceAudit) {
	qdiscBacklogSamplesByIfIndex.Range(func(_, value any) bool {
		sample := value.(*qdiscBacklogSample)
		audit := audits[sample.ifIndex]
		if audit == nil {
			audit = &qdiscInterfaceAudit{}
		}
		selectedKinds, selectedHandles := audit.describeSelectedLayers()
		core.Log.Info(nil, "Qdisc hierarchy audit",
			"interface", sample.ifName,
			"ifindex", sample.ifIndex,
			"entries", len(audit.layers),
			"aggregation", qdiscDataQsfAggregation,
			"dataQsfRawBytes", audit.selected.counters.bytes,
			"dataQsfRawPackets", audit.selected.counters.packets,
			"selectedEntries", audit.selected.entries,
			"selectedKinds", selectedKinds,
			"selectedHandles", selectedHandles,
			"allLayerSumBytes", audit.rootBytes+audit.nonRootBytes,
			"allLayerSumPackets", audit.rootPackets+audit.nonRootPackets,
			"rootBytes", audit.rootBytes,
			"rootPackets", audit.rootPackets,
			"nonRootBytes", audit.nonRootBytes,
			"nonRootPackets", audit.nonRootPackets,
			"maxLayerBytes", audit.maxLayerBytes,
			"maxLayerPackets", audit.maxLayerPkts,
			"possibleDoubleCount", audit.possibleDoubleCount(),
			"layers", audit.describeLayers())
		return true
	})
}

func formatTcHandle(handle uint32) string {
	if handle == tcHandleRoot {
		return "root"
	}
	return fmt.Sprintf("%x:%x", handle>>16, handle&0xffff)
}

func align4(n int) int {
	return (n + 3) &^ 3
}

var native binary.ByteOrder = binary.LittleEndian
