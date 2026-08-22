//go:build linux

package face

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/named-data/ndnd/fw/core"
)

const (
	qdiscDiagnosticsEnvironment    = "MARS_QDISC_DIAGNOSTICS"
	qdiscHierarchyAuditEnvironment = "MARS_QDISC_HIERARCHY_AUDIT"
	qdiscDiagnosticsReportInterval = 5 * time.Second
	qdiscHierarchyAuditInterval    = 5 * time.Second
	qdiscSlowDumpThreshold         = time.Millisecond
)

var qdiscSamplerDiagnostics = newQdiscSamplerDiagnostics(
	parseDiagnosticBool(os.Getenv(qdiscDiagnosticsEnvironment)),
)

var qdiscHierarchyAuditEnabled = parseDiagnosticBool(
	os.Getenv(qdiscHierarchyAuditEnvironment),
)

type qdiscDiagnosticCounters struct {
	samples               atomic.Uint64
	errors                atomic.Uint64
	totalDurationNs       atomic.Uint64
	maxDurationNs         atomic.Uint64
	slowSamples           atomic.Uint64
	readBufferAllocations atomic.Uint64
	netlinkSocketOpens    atomic.Uint64
}

type qdiscDiagnosticInterval struct {
	samples               uint64
	errors                uint64
	totalDurationNs       uint64
	maxDurationNs         uint64
	slowSamples           uint64
	readBufferAllocations uint64
	netlinkSocketOpens    uint64
}

type udpSNMPCounters struct {
	inDatagrams  uint64
	inErrors     uint64
	rcvbufErrors uint64
	sndbufErrors uint64
}

type qdiscResourceSnapshot struct {
	capturedAt       time.Time
	totalAllocBytes  uint64
	heapAllocBytes   uint64
	mallocs          uint64
	numGC            uint64
	pauseTotalNs     uint64
	processCPUTimeNs uint64
	processCPUValid  bool
	udp              udpSNMPCounters
	udpValid         bool
}

func parseDiagnosticBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func newQdiscSamplerDiagnostics(enabled bool) *qdiscDiagnosticCounters {
	if !enabled {
		return nil
	}
	return &qdiscDiagnosticCounters{}
}

func (diagnostics *qdiscDiagnosticCounters) recordSample(duration time.Duration, err error) {
	durationNs := uint64(duration)
	diagnostics.samples.Add(1)
	diagnostics.totalDurationNs.Add(durationNs)
	if err != nil {
		diagnostics.errors.Add(1)
	}
	if duration >= qdiscSlowDumpThreshold {
		diagnostics.slowSamples.Add(1)
	}

	for {
		previous := diagnostics.maxDurationNs.Load()
		if durationNs <= previous || diagnostics.maxDurationNs.CompareAndSwap(previous, durationNs) {
			break
		}
	}
}

func (diagnostics *qdiscDiagnosticCounters) recordReadBufferAllocation() {
	diagnostics.readBufferAllocations.Add(1)
}

func (diagnostics *qdiscDiagnosticCounters) recordNetlinkSocketOpen() {
	diagnostics.netlinkSocketOpens.Add(1)
}

func (diagnostics *qdiscDiagnosticCounters) takeInterval() qdiscDiagnosticInterval {
	return qdiscDiagnosticInterval{
		samples:               diagnostics.samples.Swap(0),
		errors:                diagnostics.errors.Swap(0),
		totalDurationNs:       diagnostics.totalDurationNs.Swap(0),
		maxDurationNs:         diagnostics.maxDurationNs.Swap(0),
		slowSamples:           diagnostics.slowSamples.Swap(0),
		readBufferAllocations: diagnostics.readBufferAllocations.Swap(0),
		netlinkSocketOpens:    diagnostics.netlinkSocketOpens.Swap(0),
	}
}

func runQdiscSamplerDiagnosticReporter(
	diagnostics *qdiscDiagnosticCounters,
	previous qdiscResourceSnapshot,
) {
	core.Log.Info(nil, "Qdisc sampler diagnostics enabled",
		"reportInterval", qdiscDiagnosticsReportInterval,
		"pollInterval", qdiscBacklogPollInterval,
		"persistentNetlink", true,
		"netlinkReadBufferBytes", qdiscNetlinkReadBufferSize,
		"hierarchyAudit", qdiscHierarchyAuditEnabled,
		"udpCounters", "/proc/net/snmp")

	ticker := time.NewTicker(qdiscDiagnosticsReportInterval)
	defer ticker.Stop()

	for !core.ShouldQuit {
		<-ticker.C
		current := captureQdiscResourceSnapshot()
		interval := diagnostics.takeInterval()
		logQdiscSamplerDiagnostic(interval, previous, current)
		previous = current
	}
}

func captureQdiscResourceSnapshot() qdiscResourceSnapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	snapshot := qdiscResourceSnapshot{
		capturedAt:      time.Now(),
		totalAllocBytes: mem.TotalAlloc,
		heapAllocBytes:  mem.HeapAlloc,
		mallocs:         mem.Mallocs,
		numGC:           uint64(mem.NumGC),
		pauseTotalNs:    mem.PauseTotalNs,
	}

	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err == nil {
		snapshot.processCPUTimeNs = timevalNanoseconds(usage.Utime) + timevalNanoseconds(usage.Stime)
		snapshot.processCPUValid = true
	}

	if udp, err := readUDPSNMPCounters("/proc/net/snmp"); err == nil {
		snapshot.udp = udp
		snapshot.udpValid = true
	}
	return snapshot
}

func logQdiscSamplerDiagnostic(
	interval qdiscDiagnosticInterval,
	previous qdiscResourceSnapshot,
	current qdiscResourceSnapshot,
) {
	elapsed := current.capturedAt.Sub(previous.capturedAt)
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		return
	}

	dumpRateHz := float64(interval.samples) / elapsedSeconds
	dumpBusyPercent := 100 * float64(interval.totalDurationNs) / float64(elapsed)
	averageDumpUs := float64(0)
	if interval.samples > 0 {
		averageDumpUs = float64(interval.totalDurationNs) / float64(interval.samples) / 1e3
	}

	cpuTimeNs := uint64(0)
	cpuPercent := float64(0)
	cpuValid := previous.processCPUValid && current.processCPUValid
	if cpuValid {
		cpuTimeNs = counterDelta(current.processCPUTimeNs, previous.processCPUTimeNs)
		cpuPercent = 100 * float64(cpuTimeNs) / float64(elapsed)
	}

	udp := udpSNMPCounters{}
	udpValid := previous.udpValid && current.udpValid
	if udpValid {
		udp = udpSNMPCounters{
			inDatagrams:  counterDelta(current.udp.inDatagrams, previous.udp.inDatagrams),
			inErrors:     counterDelta(current.udp.inErrors, previous.udp.inErrors),
			rcvbufErrors: counterDelta(current.udp.rcvbufErrors, previous.udp.rcvbufErrors),
			sndbufErrors: counterDelta(current.udp.sndbufErrors, previous.udp.sndbufErrors),
		}
	}

	core.Log.Info(nil, "Qdisc sampler diagnostic",
		"intervalMs", elapsed.Milliseconds(),
		"interfaces", countRegisteredQdiscInterfaces(),
		"dumps", interval.samples,
		"dumpErrors", interval.errors,
		"dumpRateHz", dumpRateHz,
		"dumpBusyPct", dumpBusyPercent,
		"dumpAvgUs", averageDumpUs,
		"dumpMaxUs", float64(interval.maxDurationNs)/1e3,
		"dumpsGe1ms", interval.slowSamples,
		"readBufferAllocs", interval.readBufferAllocations,
		"netlinkSocketOpens", interval.netlinkSocketOpens,
		"estimatedReadBufferAllocBytes", interval.readBufferAllocations*qdiscNetlinkReadBufferSize,
		"goTotalAllocBytes", counterDelta(current.totalAllocBytes, previous.totalAllocBytes),
		"goHeapAllocBytes", current.heapAllocBytes,
		"goMallocs", counterDelta(current.mallocs, previous.mallocs),
		"goGCs", counterDelta(current.numGC, previous.numGC),
		"goGCPauseNs", counterDelta(current.pauseTotalNs, previous.pauseTotalNs),
		"processCPUValid", cpuValid,
		"processCPUTimeMs", float64(cpuTimeNs)/1e6,
		"processCPUPct", cpuPercent,
		"udpCountersValid", udpValid,
		"udpInDatagrams", udp.inDatagrams,
		"udpInErrors", udp.inErrors,
		"udpRcvbufErrors", udp.rcvbufErrors,
		"udpSndbufErrors", udp.sndbufErrors)
}

func countRegisteredQdiscInterfaces() int {
	count := 0
	qdiscBacklogSamplesByIfIndex.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func timevalNanoseconds(value syscall.Timeval) uint64 {
	return uint64(value.Sec)*uint64(time.Second) + uint64(value.Usec)*uint64(time.Microsecond)
}

func counterDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

func readUDPSNMPCounters(path string) (udpSNMPCounters, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return udpSNMPCounters{}, err
	}
	return parseUDPSNMPCounters(contents)
}

func parseUDPSNMPCounters(contents []byte) (udpSNMPCounters, error) {
	lines := strings.Split(string(contents), "\n")
	for index := 0; index+1 < len(lines); index++ {
		headings := strings.Fields(lines[index])
		values := strings.Fields(lines[index+1])
		if len(headings) == 0 || headings[0] != "Udp:" || len(values) == 0 || values[0] != "Udp:" {
			continue
		}
		if len(headings) != len(values) {
			return udpSNMPCounters{}, fmt.Errorf(
				"UDP SNMP heading/value count mismatch: %d != %d",
				len(headings), len(values),
			)
		}

		parsed := make(map[string]uint64, len(headings)-1)
		for fieldIndex := 1; fieldIndex < len(headings); fieldIndex++ {
			value, err := strconv.ParseUint(values[fieldIndex], 10, 64)
			if err != nil {
				return udpSNMPCounters{}, fmt.Errorf(
					"invalid UDP SNMP value for %s: %w", headings[fieldIndex], err,
				)
			}
			parsed[headings[fieldIndex]] = value
		}

		required := []string{"InDatagrams", "InErrors", "RcvbufErrors", "SndbufErrors"}
		for _, field := range required {
			if _, ok := parsed[field]; !ok {
				return udpSNMPCounters{}, fmt.Errorf("UDP SNMP field %s is missing", field)
			}
		}
		return udpSNMPCounters{
			inDatagrams:  parsed["InDatagrams"],
			inErrors:     parsed["InErrors"],
			rcvbufErrors: parsed["RcvbufErrors"],
			sndbufErrors: parsed["SndbufErrors"],
		}, nil
	}
	return udpSNMPCounters{}, errors.New("UDP section not found in SNMP counters")
}
