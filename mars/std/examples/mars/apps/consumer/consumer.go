package main

import (
	"container/heap"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/named-data/ndnd/std/datapacket"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
	"github.com/named-data/ndnd/std/utils"
)

// --- Configuration Constants ---

const (
	// Global Safety Limits
	TOTAL_DURATION    = 30 * time.Second
	INTEREST_LIFETIME = 4 * time.Second
	TICKER_INTERVAL   = 1 * time.Millisecond

	// RTT & Retransmission Defaults (DT Only)
	RTT_WINDOW_DURATION   = 600 * time.Millisecond
	MIN_RTO               = 240.0
	MAX_RTO               = 3500.0
	RTO_JITTER_RATIO      = 0.10
	RTO_ESTIMATOR_FORMULA = "M*(meanRTT+4*stdDev)"
	// A single RTO is expected at non-zero random loss and only retransmits the
	// affected sequence. Repeated RTOs inside one locally measured RTT epoch are
	// treated as congestion and cause one rate/window reduction for that epoch.
	DT_TIMEOUT_BURST_THRESHOLD    = 3
	DT_TIMEOUT_EPOCH_MIN_DURATION = 100 * time.Millisecond
	DT_TIMEOUT_RATE_BACKOFF       = 0.85
	DT_TIMEOUT_WINDOW_BACKOFF     = 0.75
	// The congestion window is ACK-clocked and therefore discovers available
	// capacity without a topology-provided bandwidth value. These are protocol
	// safety bounds, not estimates of a particular emulated link.
	DT_INFLIGHT_INITIAL_RTT_MULTIPLIER = 2.0
	DT_INFLIGHT_MIN_PACKETS            = 16
	DT_INFLIGHT_MAX_PACKETS            = 512
	DT_SEND_BURST_INTERVAL             = 5 * time.Millisecond
	// Static DT control period used as the BW-estimator cadence reference.
	DT_STATIC_CONTROL_PERIOD = 20 * time.Millisecond
	// Set to true to force a fixed consumer DT control period. When false, the
	// control period is derived from RTT bootstrap and clamped to at least the
	// static period.
	DT_USE_STATIC_CONTROL_PERIOD = true
	// DT control-update period = DT_CONTROL_PERIOD_RTT_SCALE * avg bootstrap RTT.
	DT_CONTROL_PERIOD_RTT_SCALE = 1.0
	// Enable startup InterestQsf bias learning/removal on the consumer.
	DT_USE_QSF_BIAS_REMOVAL = false
)

// --- Path Discovery (PD) Configuration ---
const (
	// Max pd rate is 20000 interests/sec
	PD_INIT_RATE        = 1000.0
	PD_INIT_RTO         = 200.0
	PD_REHEARSAL_BUDGET = 20
)

// --- Data Transmission (DT) Configuration ---
const (
	RC_RTT_LOW_THRESH_MULT  = 3.0
	RC_RTT_HIGH_THRESH_MULT = 6.0
	RC_INC_FACTOR           = 1.05
	RC_DEC_FACTOR           = 0.95
	RC_CAP_LOWER_MULT       = 0.8
	RC_CAP_UPPER_MULT       = 1.75
)

type ConsumerRateControlMode int

const (
	ConsumerRateControlDelay ConsumerRateControlMode = iota
	ConsumerRateControlQsf
)

type ConsumerQsfRateControlAction int

const (
	ConsumerQsfActionEmergencyDecrease ConsumerQsfRateControlAction = iota
	ConsumerQsfActionHoldDraining
	ConsumerQsfActionGentleDecrease
	ConsumerQsfActionCautiousIncrease
	ConsumerQsfActionAggressiveProbe
	ConsumerQsfActionBWCapped
	ConsumerQsfActionBWBoosted
	ConsumerQsfActionGlobalMinCapped
	ConsumerQsfActionGlobalMaxCapped
)

type ConsumerQsfRateControlParams struct {
	MaxQueueSize      float64
	QueueAlpha        float64
	QueueBeta         float64
	MDFactor          float64
	RPFactor          float64
	GDFactor          float64
	CIFactor          float64
	SlopeThreshold    float64
	MaxBWSafetyRatio  float64
	MinBWSafetyRatio  float64
	MinRate           float64
	MaxRate           float64
	SlopeTauRatio     float64
	NoSampleWarnEvery time.Duration
}

var consumerRateControlMode = ConsumerRateControlQsf

// RTO1 is the validated release configuration. It is intentionally fixed so
// ordinary builds cannot select one of the retired sweep variants.
const rtoOuterMultiplier = 1

var consumerQsfRateControlParams = ConsumerQsfRateControlParams{
	MaxQueueSize:      1.0,
	QueueAlpha:        0.5,
	QueueBeta:         0.8,
	MDFactor:          0.9,
	RPFactor:          1.05,
	GDFactor:          0.95,
	CIFactor:          1.02,
	SlopeThreshold:    0.1,
	MaxBWSafetyRatio:  1.2,
	MinBWSafetyRatio:  0.5,
	MinRate:           0.005,
	MaxRate:           0.0,
	SlopeTauRatio:     1.0 / 3.0,
	NoSampleWarnEvery: 500 * time.Millisecond,
}

// Scale the packet count and rates from a fixed reference byte budget to the
// serialized Data payload size used by the current workload.
const (
	baseMaxPackets           = 15000
	referenceDtInitRatePps   = 3000.0
	referenceDtMinRatePps    = 10.0
	referenceDataPacketBytes = 16 + (150 * 8)
	targetDtInitMbps         = (referenceDtInitRatePps * referenceDataPacketBytes * 8.0) / 1e6
	targetDtMinRateMbps      = (referenceDtMinRatePps * referenceDataPacketBytes * 8.0) / 1e6
	baseThroughputWindow     = 20 * time.Millisecond //! Sliding window duration
	consumerQsfSampleLimit   = 3
	consumerBwSampleMin      = 2
	//! Bw estimation
	consumerBwObsLimit   = 3
	consumerBwAlphaUp    = 0.25
	consumerBwAlphaDown  = 0.25
	consumerBwAlphaSmall = 0.10
)

const consumerStatsCsvDir = "std/examples/mars/logs"
const consumerThroughputSampleInterval = 50 * time.Millisecond
const consumerThroughputSampleInterval100 = 100 * time.Millisecond

var (
	MAX_PACKETS            = baseMaxPackets
	DT_INIT_RATE           = referenceDtInitRatePps
	THROUGHPUT_WINDOW_DUR  = baseThroughputWindow
	RC_MIN_RATE            = referenceDtMinRatePps
	consumerResolvedLogDir string
	consumerLogDirOnce     sync.Once
)

const oddConsumerDtDelay = 1001500 * time.Microsecond

type ConsumerTag struct{}

func (ConsumerTag) String() string { return "NDN-Consumer" }

var consumerTag = ConsumerTag{}

func consumerLogFloat1(v float64) float64 {
	return math.Round(v*10) / 10
}

func rateMbpsToPps(rateMbps float64, payloadBytes float64) float64 {
	if rateMbps <= 0 || payloadBytes <= 0 {
		return 0
	}
	return (rateMbps * 1e6) / (payloadBytes * 8.0)
}

func selectConsumerRateControlMode() ConsumerRateControlMode {
	if os.Getenv("MARS_DEPLOYMENT_MODE") == "minimal" {
		return ConsumerRateControlDelay
	}
	return ConsumerRateControlQsf
}

type TransmissionMode int

const (
	ModePD TransmissionMode = iota
	ModeDT
)

var (
	// Global mode setting for initialization
	initialMode      = ModePD
	consumerNodeName string
	consumerIndex    int
	consumerCsvStart time.Time
	consumerCsvMu    sync.Mutex

	// Global coordination for the PD-to-DT transition.
	pdFinishedCount int
	pdTotalFlows    int
	pdMutex         sync.Mutex
	startDtCh       chan struct{}
)

func applyAdaptiveDtTunables(flowCount int) {
	currentPktSize := datapacket.NewDataPacket().GetSize()
	if currentPktSize <= 0 {
		log.Warn(consumerTag, "Adaptive DT tuning disabled: invalid packet size", "size", currentPktSize)
		return
	}

	payloadBytes := float64(currentPktSize)
	scale := float64(referenceDataPacketBytes) / payloadBytes
	if scale <= 0 {
		log.Warn(consumerTag, "Adaptive DT tuning disabled: invalid scale", "scale", scale)
		return
	}

	effectiveInitMbps := targetDtInitMbps
	deploymentMode := os.Getenv("MARS_DEPLOYMENT_MODE")
	if deploymentMode == "minimal" && flowCount > 0 {
		effectiveInitMbps = targetDtInitMbps / float64(flowCount)
	}

	DT_INIT_RATE = math.Max(1.0, rateMbpsToPps(effectiveInitMbps, payloadBytes))
	RC_MIN_RATE = math.Max(1.0, rateMbpsToPps(targetDtMinRateMbps, payloadBytes))
	MAX_PACKETS = int(math.Max(1.0, math.Round(float64(baseMaxPackets)*scale)))
	// Keep DT sliding-window duration static (no chunk-size shaping).
	THROUGHPUT_WINDOW_DUR = baseThroughputWindow

	log.Info(consumerTag, "Adaptive DT tuning applied",
		"deploymentMode", deploymentMode,
		"flowCount", flowCount,
		"dataPacketSizeBytes", currentPktSize,
		"scale", scale,
		"DT_INIT_RATE_Pps", consumerLogFloat1(DT_INIT_RATE),
		"DT_INIT_RATE_Mbps", consumerLogFloat1(ratePpsToMbps(DT_INIT_RATE, payloadBytes)),
		"RC_MIN_RATE_Pps", consumerLogFloat1(RC_MIN_RATE),
		"RC_MIN_RATE_Mbps", consumerLogFloat1(ratePpsToMbps(RC_MIN_RATE, payloadBytes)),
		"MAX_PACKETS", MAX_PACKETS,
		"THROUGHPUT_WINDOW_DUR", THROUGHPUT_WINDOW_DUR)
}

// --- Packet Structures ---

type PacketSample struct {
	Timestamp   time.Time
	Size        int
	RTT         float64
	RTTValid    bool
	InterestQsf float64
	DataQsf     float64
}

type bwTrend int

const (
	bwTrendUnknown bwTrend = iota
	bwTrendIncreasing
	bwTrendDecreasing
	bwTrendFluctuating
)

type RTTSample struct {
	Timestamp time.Time
	RTT       float64
}

type PendingSend struct {
	SendTime      time.Time
	Retransmitted bool
}

type flowCsvSink struct {
	mu            sync.Mutex
	path          string
	file          *os.File
	writer        *csv.Writer
	headerWritten bool
}

type nodeThroughputCsvSink struct {
	mu            sync.Mutex
	path          string
	file          *os.File
	writer        *csv.Writer
	headerWritten bool
}

type FlowContext struct {
	mu       sync.Mutex
	Prefix   string
	baseName enc.Name
	Mode     TransmissionMode

	// --- Shared State ---
	currentRate  float64
	rto          float64
	pendingSends map[string]PendingSend

	totalBytes        int64
	pktsReceivedTotal int
	receivedSeqs      map[int]bool
	startTime         time.Time
	endTime           time.Time
	p95Time           time.Time
	p99Time           time.Time
	p100Time          time.Time
	isFinished        bool
	doneCh            chan struct{}

	// --- Path Discovery (PD) Specific State ---
	CurrentTier    int
	IsPaused       bool
	StopPD         bool
	OutstandingPD  int
	RehearsalCount int
	HasRehearsed   bool
	PdStartTime    time.Time
	PdFinished     bool

	// --- Data Transmission (DT) Specific State ---
	rttHistory            []RTTSample
	srtt, rttVar          float64
	window                []PacketSample
	estimatedBandwidth    float64
	lastMeasuredBandwidth float64
	lastBWUpdateRule      string
	lastInterestQsfAvg    float64
	lastInterestQsfSlope  float64
	lastDataQsfAvg        float64
	lastDataQsfSlope      float64
	lastQsfObservationOk  bool
	qsfWindowWarmed       bool
	interestQsfBias       float64
	interestQsfBiasSum    float64
	interestQsfBiasCount  int
	interestQsfBiasReady  bool
	bwCapActive           bool
	bwCapEligible         bool
	bwBootstrapReady      bool
	bwObservations        []float64
	lastBwObservationTime time.Time
	lastRateSampleTime    time.Time
	lastNoSampleWarn      time.Time
	timeoutEpochStart     time.Time
	timeoutEpochEvents    int
	timeoutEpochBackoff   bool
	timeoutEvents         int
	timeoutBackoffs       int
	congestionWindow      float64
	maxInflight           int
	pktsCalibrated        int
	initialRTTSum         float64
	baseRTT               float64
	adjustmentPeriod      time.Duration
	lastRateUpdate        time.Time
	interestsSentInPeriod int
	retxQueue             []int
	retxHead              int
	retxQueued            map[int]bool
	firstDtDataLogged     bool
	lastCsvPayloadBytes   float64
	lastCsvBWMbps         float64
	lastCsvPrevRateMbps   float64
	lastCsvActualRateMbps float64
	lastCsvNewRateMbps    float64
	lastCsvMetricsValid   bool
	shadowLoss            shadowLossDetector

	// Reset signal for runFlow loop
	resetSignal chan struct{}
	csvSink     *flowCsvSink
}

type FlowProgressSnapshot struct {
	Prefix     string
	Mode       TransmissionMode
	StartTime  time.Time
	TotalBytes int64
	PktsRecv   int
	IsFinished bool
}

type FlowFinalStats struct {
	ThroughputMbps  float64
	TotalBytes      int64
	Duration        float64
	FCT95           float64
	FCT99           float64
	FCT100          float64
	Complete        bool
	ReceivedPackets int
	ExpectedPackets int
	MissingPackets  int
	RTOExpirations  int
	RateBackoffs    int
	MaxInflight     int
	FinalCwnd       float64
}

var nodeThroughputSinks = make(map[time.Duration]*nodeThroughputCsvSink)

func NewFlowContext(prefix string) *FlowContext {
	now := time.Now()

	f := &FlowContext{
		Prefix:       prefix,
		startTime:    now,
		endTime:      now,
		doneCh:       make(chan struct{}),
		pendingSends: make(map[string]PendingSend),
		receivedSeqs: make(map[int]bool),
		retxQueue:    make([]int, 0),
		retxHead:     0,
		retxQueued:   make(map[int]bool),
		resetSignal:  make(chan struct{}, 1),
	}

	if initialMode == ModePD {
		f.Mode = ModePD
		f.currentRate = PD_INIT_RATE
		f.rto = PD_INIT_RTO

		f.CurrentTier = 0
		f.IsPaused = false
		f.StopPD = false
		f.OutstandingPD = 0
		f.RehearsalCount = 0
		f.HasRehearsed = false
		f.PdStartTime = now
		f.PdFinished = false
		f.updatePDBaseName(0)
	} else {
		f.Mode = ModeDT
		f.initDTState()
	}

	return f
}

func (f *FlowContext) initDTState() {
	now := time.Now()

	f.currentRate = DT_INIT_RATE
	f.rto = MIN_RTO * 2

	f.startTime = now
	f.endTime = now
	f.totalBytes = 0
	f.pktsReceivedTotal = 0
	f.p95Time = time.Time{}
	f.p99Time = time.Time{}
	f.p100Time = time.Time{}
	f.isFinished = false
	f.pendingSends = make(map[string]PendingSend)
	f.receivedSeqs = make(map[int]bool)
	f.retxQueue = make([]int, 0)
	f.retxHead = 0
	f.retxQueued = make(map[int]bool)
	f.firstDtDataLogged = false
	f.rttHistory = make([]RTTSample, 0)
	f.estimatedBandwidth = DT_INIT_RATE
	f.lastMeasuredBandwidth = 0
	f.lastBWUpdateRule = "init"
	f.lastInterestQsfAvg = 0
	f.lastInterestQsfSlope = 0
	f.lastDataQsfAvg = 0
	f.lastDataQsfSlope = 0
	f.lastQsfObservationOk = false
	f.qsfWindowWarmed = false
	f.interestQsfBias = 0
	f.interestQsfBiasSum = 0
	f.interestQsfBiasCount = 0
	f.interestQsfBiasReady = false
	f.bwCapActive = false
	f.bwCapEligible = false
	f.bwBootstrapReady = false
	f.bwObservations = make([]float64, 0, consumerBwObsLimit)
	f.lastBwObservationTime = time.Time{}
	f.lastRateSampleTime = time.Time{}
	f.lastNoSampleWarn = time.Time{}
	f.timeoutEpochStart = time.Time{}
	f.timeoutEpochEvents = 0
	f.timeoutEpochBackoff = false
	f.timeoutEvents = 0
	f.timeoutBackoffs = 0
	f.congestionWindow = initialDTInflightWindow(DT_INIT_RATE, 0)
	f.maxInflight = 0
	f.window = make([]PacketSample, 0)
	f.pktsCalibrated = 0
	f.initialRTTSum = 0
	f.baseRTT = 0
	f.srtt = 0
	f.rttVar = 0
	f.interestsSentInPeriod = 0
	f.lastRateUpdate = now
	f.shadowLoss.reset()

	n, _ := enc.NameFromStr(f.Prefix)
	if consumerNodeName != "" {
		n = n.Append(enc.NewStringComponent(0x08, consumerNodeName))
	}
	n = n.Append(enc.NewStringComponent(0x08, "dt"))
	f.baseName = n
}

func flowPrefixCsvLabel(prefix string) string {
	label := strings.TrimPrefix(prefix, "/")
	label = strings.ReplaceAll(label, "/", "_")
	if label == "" {
		return "unknown"
	}
	return label
}

func consumerStatsDir() string {
	consumerLogDirOnce.Do(func() {
		if envDir := strings.TrimSpace(os.Getenv("MARS_LOG_DIR")); envDir != "" {
			consumerResolvedLogDir = envDir
		} else {
			consumerResolvedLogDir = consumerStatsCsvDir
		}
	})
	return consumerResolvedLogDir
}

func (f *FlowContext) getFlowCsvSink() *flowCsvSink {
	if consumerNodeName == "" {
		return nil
	}
	if f.csvSink != nil {
		return f.csvSink
	}
	f.csvSink = &flowCsvSink{
		path: filepath.Join(consumerStatsDir(), consumerNodeName+"_"+flowPrefixCsvLabel(f.Prefix)+".csv"),
	}
	return f.csvSink
}

func getNodeThroughputCsvSink(sampleInterval time.Duration) *nodeThroughputCsvSink {
	if consumerNodeName == "" {
		return nil
	}
	if sink, ok := nodeThroughputSinks[sampleInterval]; ok {
		return sink
	}
	sampleLabel := strconv.FormatInt(sampleInterval.Milliseconds(), 10)
	sink := &nodeThroughputCsvSink{
		path: filepath.Join(consumerStatsDir(), consumerNodeName+"_throughput_"+sampleLabel+"ms.csv"),
	}
	nodeThroughputSinks[sampleInterval] = sink
	return sink
}

func consumerCsvFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}

func consumerElapsedMs(now time.Time) float64 {
	consumerCsvMu.Lock()
	defer consumerCsvMu.Unlock()
	if consumerCsvStart.IsZero() {
		consumerCsvStart = now
	}
	return now.Sub(consumerCsvStart).Seconds() * 1000.0
}

func consumerMilestoneTarget(totalPackets int, ratio float64) int {
	if totalPackets <= 0 {
		return 0
	}
	target := int(math.Ceil(float64(totalPackets) * ratio))
	if target < 1 {
		target = 1
	}
	if target > totalPackets {
		target = totalPackets
	}
	return target
}

func consumerDurationSeconds(start time.Time, end time.Time) float64 {
	if start.IsZero() || end.IsZero() {
		return 0
	}
	duration := end.Sub(start).Seconds()
	if duration < 0 {
		return 0
	}
	return duration
}

func ratePpsToMbps(ratePps float64, payloadBytes float64) float64 {
	if ratePps <= 0 || payloadBytes <= 0 {
		return 0
	}
	return (ratePps * payloadBytes * 8.0) / 1e6
}

func (f *FlowContext) writeRateControlCsv(
	now time.Time,
	interestQ float64,
	interestSlope float64,
	dataQ float64,
	bwPps float64,
	bwSamples int,
	prevRate float64,
	actualRate float64,
	newRate float64,
	controlState string,
) {
	sink := f.getFlowCsvSink()
	if sink == nil {
		return
	}

	elapsedMs := consumerElapsedMs(now)

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if sink.writer == nil {
		if err := os.MkdirAll(filepath.Dir(sink.path), 0o755); err != nil {
			log.Warn(consumerTag, "Failed to create consumer CSV directory", "path", sink.path, "err", err)
			return
		}
		file, err := os.OpenFile(sink.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			log.Warn(consumerTag, "Failed to open consumer CSV", "path", sink.path, "err", err)
			return
		}
		sink.file = file
		sink.writer = csv.NewWriter(file)
	}

	if !sink.headerWritten {
		header := []string{
			"time_ms",
			"interest_q",
			"interest_slope",
			"data_q",
			"bw_est_mbps",
			"bw_samples",
			"send_prev_mbps",
			"send_actual_prev_mbps",
			"send_new_mbps",
			"control_state",
		}
		if err := sink.writer.Write(header); err != nil {
			log.Warn(consumerTag, "Failed to write consumer CSV header", "path", sink.path, "err", err)
			return
		}
		sink.headerWritten = true
	}

	payloadBytes := f.windowPayloadAvgBytes()
	csvPayloadBytes := payloadBytes
	if csvPayloadBytes <= 0 && f.lastCsvPayloadBytes > 0 {
		csvPayloadBytes = f.lastCsvPayloadBytes
	}
	bwMbps := ratePpsToMbps(bwPps, csvPayloadBytes)
	prevRateMbps := ratePpsToMbps(prevRate, csvPayloadBytes)
	actualRateMbps := ratePpsToMbps(actualRate, csvPayloadBytes)
	newRateMbps := ratePpsToMbps(newRate, csvPayloadBytes)
	if controlState == "frozen" && len(f.window) == 0 && f.lastCsvMetricsValid {
		bwMbps = f.lastCsvBWMbps
		prevRateMbps = f.lastCsvPrevRateMbps
		newRateMbps = f.lastCsvNewRateMbps
	}
	row := []string{
		consumerCsvFloat(elapsedMs),
		consumerCsvFloat(interestQ),
		consumerCsvFloat(interestSlope),
		consumerCsvFloat(dataQ),
		consumerCsvFloat(bwMbps),
		strconv.Itoa(bwSamples),
		consumerCsvFloat(prevRateMbps),
		consumerCsvFloat(actualRateMbps),
		consumerCsvFloat(newRateMbps),
		controlState,
	}
	if err := sink.writer.Write(row); err != nil {
		log.Warn(consumerTag, "Failed to write consumer CSV row", "path", sink.path, "err", err)
		return
	}
	f.lastCsvBWMbps = bwMbps
	f.lastCsvPrevRateMbps = prevRateMbps
	f.lastCsvActualRateMbps = actualRateMbps
	f.lastCsvNewRateMbps = newRateMbps
	if payloadBytes > 0 {
		f.lastCsvPayloadBytes = payloadBytes
	}
	f.lastCsvMetricsValid = true
	sink.writer.Flush()
	if err := sink.writer.Error(); err != nil {
		log.Warn(consumerTag, "Failed to flush consumer CSV", "path", sink.path, "err", err)
	}
}

func (f *FlowContext) updatePDBaseName(tier int) {
	n, _ := enc.NameFromStr(f.Prefix)
	if consumerNodeName != "" {
		n = n.Append(enc.NewStringComponent(0x08, consumerNodeName))
	}
	n = n.Append(enc.NewStringComponent(0x08, "pd"))
	n = n.Append(enc.NewStringComponent(0x08, strconv.Itoa(tier)))
	f.baseName = n
}

func (f *FlowContext) SnapshotProgress() FlowProgressSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return FlowProgressSnapshot{
		Prefix:     f.Prefix,
		Mode:       f.Mode,
		StartTime:  f.startTime,
		TotalBytes: f.totalBytes,
		PktsRecv:   f.pktsReceivedTotal,
		IsFinished: f.isFinished,
	}
}

func (f *FlowContext) GetRate() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentRate
}

func (f *FlowContext) GetRTO() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rto
}

func (f *FlowContext) GetRTOIfPending(name string, seq int) (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, pending := f.pendingSends[name]; !pending {
		return 0, false
	}
	if f.Mode == ModeDT && (f.isFinished || f.receivedSeqs[seq]) {
		return 0, false
	}
	return f.rto, true
}

func clampFloat(value float64, minimum float64, maximum float64) float64 {
	if value < minimum {
		return minimum
	}
	if maximum > 0 && value > maximum {
		return maximum
	}
	return value
}

func initialDTInflightWindow(rate float64, rttMs float64) float64 {
	if rate <= 0 {
		rate = DT_INIT_RATE
	}
	if rttMs <= 0 {
		rttMs = MIN_RTO / 4
	}
	window := rate * (rttMs / 1000.0) * DT_INFLIGHT_INITIAL_RTT_MULTIPLIER
	return clampFloat(window, DT_INFLIGHT_MIN_PACKETS, DT_INFLIGHT_MAX_PACKETS)
}

func (f *FlowContext) dtInflightLimitLocked() int {
	window := f.congestionWindow
	if window <= 0 {
		window = initialDTInflightWindow(f.currentRate, f.baseRTT)
	}
	limit := int(math.Ceil(window))
	if limit < DT_INFLIGHT_MIN_PACKETS {
		limit = DT_INFLIGHT_MIN_PACKETS
	}
	if limit > DT_INFLIGHT_MAX_PACKETS {
		limit = DT_INFLIGHT_MAX_PACKETS
	}
	return limit
}

// THREAD SAFETY: caller must hold f.mu.
func (f *FlowContext) growCongestionWindowOnAckLocked() {
	if f.congestionWindow <= 0 {
		f.congestionWindow = initialDTInflightWindow(f.currentRate, f.baseRTT)
	}
	if f.congestionWindow >= DT_INFLIGHT_MAX_PACKETS {
		return
	}
	// Additive increase: approximately one packet per window of acknowledged
	// Data. This is independent of configured topology bandwidth.
	f.congestionWindow += 1.0 / math.Max(f.congestionWindow, 1.0)
	if f.congestionWindow > DT_INFLIGHT_MAX_PACKETS {
		f.congestionWindow = DT_INFLIGHT_MAX_PACKETS
	}
}

func (f *FlowContext) CanSendNewDT() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Mode != ModeDT || f.isFinished {
		return false
	}
	return len(f.pendingSends) < f.dtInflightLimitLocked()
}

// OnDTRTO records the expiration for observability but deliberately keeps rate
// control and the ACK-clocked congestion window unchanged. The application RTO
// still queues the same logical sequence for retransmission.
func (f *FlowContext) OnDTRTO(_ time.Time, _ time.Duration) (float64, float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.timeoutEvents++
	return f.currentRate, f.rto, false
}

func (f *FlowContext) RecordInterestSend(
	name string,
	seq int,
	t time.Time,
	isRetransmission bool,
) (TransmissionMode, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mode := f.Mode
	if f.Mode == ModeDT && (f.isFinished || f.receivedSeqs[seq]) {
		return mode, false
	}
	entry, exists := f.pendingSends[name]
	if !exists {
		// First transmission for this name.
		f.pendingSends[name] = PendingSend{
			SendTime:      t,
			Retransmitted: isRetransmission,
		}
	} else if isRetransmission {
		// Karn's algorithm: keep original send timestamp and mark retransmitted.
		entry.Retransmitted = true
		f.pendingSends[name] = entry
	} else {
		// Fresh transmission path (non-retransmission).
		f.pendingSends[name] = PendingSend{
			SendTime:      t,
			Retransmitted: false,
		}
	}

	if f.Mode == ModeDT {
		if isRetransmission {
			f.shadowLoss.recordRecoverySendLocked(seq)
		} else {
			f.shadowLoss.recordFreshSendLocked(seq, t)
		}
		if len(f.pendingSends) > f.maxInflight {
			f.maxInflight = len(f.pendingSends)
		}
		// Retransmissions are also token-controlled and consume link capacity, so
		// include them in the measured send rate printed by the control loop.
		f.interestsSentInPeriod++
	} else if !isRetransmission {
		f.OutstandingPD++
	}
	return mode, true
}

func (f *FlowContext) QueueRetransmission(seq int) bool {
	if seq <= 0 {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queueRetransmissionLocked(seq)
}

// THREAD SAFETY: caller must hold f.mu.
func (f *FlowContext) queueRetransmissionLocked(seq int) bool {
	if f.Mode != ModeDT || f.isFinished || f.receivedSeqs[seq] || f.retxQueued[seq] {
		return false
	}
	if _, pending := f.pendingSends[f.interestNameForSeq(seq)]; !pending {
		return false
	}
	f.retxQueued[seq] = true
	f.retxQueue = append(f.retxQueue, seq)
	return true
}

func (f *FlowContext) queueShadowFastRepair(
	candidate shadowFastRepairCandidate,
) shadowFastRepairQueueOutcome {
	f.mu.Lock()
	defer f.mu.Unlock()

	outcome := shadowFastRepairQueueStale
	pending, tracked := f.shadowLoss.pending[candidate.Sequence]
	if f.Mode == ModeDT && !f.isFinished &&
		!f.receivedSeqs[candidate.Sequence] && tracked &&
		pending.generation == candidate.Generation {
		if f.retxQueued[candidate.Sequence] {
			outcome = shadowFastRepairQueueAlreadyQueued
		} else if f.queueRetransmissionLocked(candidate.Sequence) {
			outcome = shadowFastRepairQueueQueued
		}
	}
	f.shadowLoss.recordFastRepairQueueOutcomeLocked(candidate, outcome)
	return outcome
}

func (f *FlowContext) recordShadowFastRepairOutcome(
	candidate shadowFastRepairCandidate,
	outcome shadowFastRepairQueueOutcome,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shadowLoss.recordFastRepairQueueOutcomeLocked(candidate, outcome)
}

// THREAD SAFETY: caller must hold f.mu.
func (f *FlowContext) compactRetransmissionQueueLocked() {
	if f.retxHead == 0 {
		return
	}
	if f.retxHead < len(f.retxQueue)/2 && f.retxHead < 1024 {
		return
	}
	copy(f.retxQueue[0:], f.retxQueue[f.retxHead:])
	newLen := len(f.retxQueue) - f.retxHead
	for i := newLen; i < len(f.retxQueue); i++ {
		f.retxQueue[i] = 0
	}
	f.retxQueue = f.retxQueue[:newLen]
	f.retxHead = 0
}

func (f *FlowContext) PopRetransmission() (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for f.retxHead < len(f.retxQueue) {
		seq := f.retxQueue[f.retxHead]
		f.retxQueue[f.retxHead] = 0
		f.retxHead++
		if !f.retxQueued[seq] {
			continue
		}
		delete(f.retxQueued, seq)
		if f.Mode != ModeDT || f.isFinished {
			continue
		}
		if _, pending := f.pendingSends[f.interestNameForSeq(seq)]; !pending {
			continue
		}
		f.compactRetransmissionQueueLocked()
		return seq, true
	}
	f.retxQueue = f.retxQueue[:0]
	f.retxHead = 0
	return 0, false
}

func (f *FlowContext) HasQueuedRetransmission() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.retxQueued) > 0
}

func (f *FlowContext) interestNameForSeq(seq int) string {
	return f.baseName.Append(enc.NewStringComponent(0x08, "seq-"+strconv.Itoa(seq))).String()
}

func (f *FlowContext) ClearQueuedRetransmission(seq int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clearQueuedRetransmissionLocked(seq)
}

// THREAD SAFETY: caller must hold f.mu.
func (f *FlowContext) clearQueuedRetransmissionLocked(seq int) {
	if seq <= 0 {
		return
	}
	delete(f.retxQueued, seq)
}

// --- Core Event Handlers ---

// OnNack handles Nack packets.
func (f *FlowContext) OnNack(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	log.Debug(consumerTag, "OnNack", "name", name, "mode", f.Mode)

	if f.Mode == ModePD {
		delete(f.pendingSends, name)
		if f.OutstandingPD > 0 {
			f.OutstandingPD--
		}
		log.Debug(consumerTag, "PD: Nack processed", "prefix", f.Prefix, "outstanding", f.OutstandingPD)
	}
}

// OnTimeout handles Application-Layer RTO Expiration.
// This is called by the RetransmissionController, NOT the NDN framework callback.
func (f *FlowContext) OnTimeout(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.pendingSends, name)

	if f.Mode == ModePD {
		if f.OutstandingPD > 0 {
			f.OutstandingPD--
		}

		log.Debug(consumerTag, "PD: RTO Timeout processed", "prefix", f.Prefix, "outstanding", f.OutstandingPD)

		// A final timeout can complete the current discovery tier.
		f.checkPdState()
	} else if f.Mode == ModeDT {
		log.Debug(consumerTag, "DT: RTO Timeout processed", "prefix", f.Prefix)
	}
}

// OnData handles Data packets and returns true only when the Data completed a
// currently pending Interest. Duplicate or stale Data should not inflate stats.
func (f *FlowContext) OnData(payload []byte, dataName string, receiveTime time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	pending, ok := f.pendingSends[dataName]
	if !ok {
		return false
	}

	if f.isFinished {
		delete(f.pendingSends, dataName)
		return false
	}

	// =========================================================
	//               MODE A: PATH DISCOVERY (PD)
	// =========================================================
	if f.Mode == ModePD {
		delete(f.pendingSends, dataName)
		if f.OutstandingPD > 0 {
			f.OutstandingPD--
		}

		seqNum := extractSeqNum(dataName)
		if seqNum >= 0 && seqNum%50 == 0 {
			log.Debug(consumerTag, "PD Data Received", "name", dataName, "tier", f.CurrentTier, "outstanding", f.OutstandingPD)
		}

		discoveryPkt, err := datapacket.DeserializeDiscovery(payload)
		if err != nil {
			log.Warn(consumerTag, "PD: Failed to deserialize packet", "err", err)
			return false
		}

		if discoveryPkt.NodeConverged && !f.StopPD {
			log.Debug(consumerTag, "PD: Node Converged detected", "prefix", f.Prefix)
			f.StopPD = true
		} else if discoveryPkt.TierConverged && !f.IsPaused {
			log.Debug(consumerTag, "PD: Tier Converged detected - Pausing", "prefix", f.Prefix, "tier", f.CurrentTier)
			f.IsPaused = true
		}

		if f.IsPaused && !discoveryPkt.TierConverged {
			log.Debug(consumerTag, "PD: False Convergence detected (Paused) - Resuming", "prefix", f.Prefix)
			f.IsPaused = false
			if !f.HasRehearsed {
				f.HasRehearsed = true
				f.RehearsalCount = PD_REHEARSAL_BUDGET
				log.Debug(consumerTag, "PD: Triggering Rehearsal", "prefix", f.Prefix)
			}
		}

		f.checkPdState()
		return true
	}

	// =========================================================
	//             MODE B: DATA TRANSMISSION (DT)
	// =========================================================
	if f.Mode == ModeDT {
		seqNum := extractSeqNum(dataName)
		if seqNum <= 0 || f.receivedSeqs[seqNum] {
			if f.receivedSeqs[seqNum] {
				delete(f.pendingSends, dataName)
				f.clearQueuedRetransmissionLocked(seqNum)
			}
			return false
		}
		dataPacket, err := datapacket.DeserializeData(payload)
		if err != nil {
			log.Warn(consumerTag, "DT: Failed to deserialize packet", "err", err)
			return false
		}
		f.shadowLoss.recordDataLocked(seqNum, receiveTime)
		// Invalid Data must leave the logical request pending so its application
		// RTO can retransmit it. Only valid Data completes the pending send.
		delete(f.pendingSends, dataName)
		if !f.firstDtDataLogged {
			f.firstDtDataLogged = true
			log.Debug(consumerTag, "First DT Data Received",
				"prefix", f.Prefix,
				"interestQsf", consumerLogFloat1(dataPacket.InterestQsf))
		}

		rttVal := float64(receiveTime.Sub(pending.SendTime).Nanoseconds()) / 1e6

		f.endTime = receiveTime
		f.receivedSeqs[seqNum] = true
		f.totalBytes += int64(len(payload))
		f.pktsReceivedTotal++
		if f.p95Time.IsZero() && f.pktsReceivedTotal >= consumerMilestoneTarget(MAX_PACKETS, 0.95) {
			f.p95Time = receiveTime
		}
		if f.p99Time.IsZero() && f.pktsReceivedTotal >= consumerMilestoneTarget(MAX_PACKETS, 0.99) {
			f.p99Time = receiveTime
		}
		if f.p100Time.IsZero() && f.pktsReceivedTotal >= MAX_PACKETS {
			f.p100Time = receiveTime
		}

		if f.pktsReceivedTotal >= MAX_PACKETS {
			f.isFinished = true
			close(f.doneCh)
		}

		f.clearQueuedRetransmissionLocked(seqNum)
		if seqNum >= 0 && seqNum%200 == 0 {
			log.Debug(consumerTag, "DT Data Received", "name", dataName, "rtt", rttVal, "totalRx", f.pktsReceivedTotal)
		}

		// Karn's rule applies only to RTT/RTO. A retransmitted Data packet is still
		// a valid delivery-rate and QSF observation; excluding it starves the
		// controller precisely while a flow is recovering from loss.
		rttValid := !pending.Retransmitted
		if rttValid {
			f.updateRTT(receiveTime, rttVal)
		}
		f.updateRateControl(
			receiveTime,
			len(payload),
			rttVal,
			rttValid,
			dataPacket.InterestQsf,
			dataPacket.DataQsf,
		)
		f.growCongestionWindowOnAckLocked()
		return true
	}

	return false
}

func (f *FlowContext) checkPdState() {
	if f.StopPD && f.OutstandingPD == 0 {
		if !f.PdFinished {
			duration := time.Since(f.PdStartTime)
			log.Info(consumerTag, "PD: Finished", "prefix", f.Prefix, "duration", duration)
			f.PdFinished = true
			go checkAllPdFinished()
		}
		return
	}

	if f.IsPaused && f.OutstandingPD == 0 {
		f.CurrentTier++
		log.Debug(consumerTag, "PD: Tier Finished - Probing Next", "prefix", f.Prefix, "newTier", f.CurrentTier)
		f.IsPaused = false
		f.updatePDBaseName(f.CurrentTier)
	}
}

func (f *FlowContext) updateRTT(receiveTime time.Time, rttVal float64) {
	f.rttHistory = append(f.rttHistory, RTTSample{Timestamp: receiveTime, RTT: rttVal})
	validIdx := 0
	for i, s := range f.rttHistory {
		if receiveTime.Sub(s.Timestamp) <= RTT_WINDOW_DURATION {
			validIdx = i
			break
		}
	}
	f.rttHistory = f.rttHistory[validIdx:]

	count := len(f.rttHistory)
	var meanRTT, variance, calculatedRTO float64
	if count < 2 {
		meanRTT = rttVal
		variance = 0.0
		calculatedRTO = math.Max(MIN_RTO, 2*rttVal)
	} else {
		var sum float64
		for _, s := range f.rttHistory {
			sum += s.RTT
		}
		meanRTT = sum / float64(count)
		var varSum float64
		for _, s := range f.rttHistory {
			diff := s.RTT - meanRTT
			varSum += diff * diff
		}
		variance = varSum / float64(count)
		stdDev := math.Sqrt(variance)
		calculatedRTO = meanRTT + 4*stdDev
	}
	finalRTO := applyRTOOuterMultiplier(calculatedRTO, rtoOuterMultiplier)

	f.srtt = meanRTT
	f.rttVar = variance
	f.rto = finalRTO
}

func applyRTOOuterMultiplier(calculatedRTO float64, multiplier int) float64 {
	finalRTO := float64(multiplier) * calculatedRTO
	if finalRTO < MIN_RTO {
		finalRTO = MIN_RTO
	}
	if finalRTO > MAX_RTO {
		finalRTO = MAX_RTO
	}

	return finalRTO
}

func (f *FlowContext) updateRateControl(
	receiveTime time.Time,
	payloadSize int,
	rttVal float64,
	rttValid bool,
	interestQsf float64,
	dataQsf float64,
) {
	f.observeInterestQsfBias(interestQsf)
	f.lastRateSampleTime = receiveTime
	f.window = append(f.window, PacketSample{
		Timestamp:   receiveTime,
		Size:        payloadSize,
		RTT:         rttVal,
		RTTValid:    rttValid,
		InterestQsf: interestQsf,
		DataQsf:     dataQsf,
	})
	f.trimRateControlWindow(receiveTime)
	if len(f.window) >= consumerQsfSampleLimit {
		f.qsfWindowWarmed = true
	}

	if f.pktsCalibrated < 5 {
		if !rttValid {
			return
		}
		f.initialRTTSum += rttVal
		f.pktsCalibrated++
		if f.pktsCalibrated == 5 {
			f.baseRTT = f.initialRTTSum / 5.0
			calibratedWindow := initialDTInflightWindow(f.currentRate, f.baseRTT)
			if calibratedWindow > f.congestionWindow {
				f.congestionWindow = calibratedWindow
			}
			f.adjustmentPeriod = consumerControlPeriodFromBootstrap(f.baseRTT)
			f.lastRateUpdate = receiveTime
			f.interestsSentInPeriod = 0
			log.Info(consumerTag, "Flow Calibration Complete",
				"flow", f.Prefix,
				"mode", consumerRateControlModeString(),
				"baseRTT", consumerLogFloat1(f.baseRTT),
				"staticControlPeriod", DT_USE_STATIC_CONTROL_PERIOD,
				"controlPeriodScale", DT_CONTROL_PERIOD_RTT_SCALE,
				"adjustmentPeriod", f.adjustmentPeriod)
		}
		return
	}

	switch consumerRateControlMode {
	case ConsumerRateControlQsf:
		f.updateQsfBandwidthEstimate(receiveTime, math.Max(f.correctedInterestQsf(interestQsf), dataQsf))
	default:
		f.updateDelayBandwidthEstimate(receiveTime)
	}
}

func (f *FlowContext) runPeriodicRateControl(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.Mode != ModeDT || f.pktsCalibrated < 5 || f.isFinished || f.adjustmentPeriod <= 0 || !f.bwBootstrapReady {
		return
	}

	f.trimRateControlWindow(now)
	timeSinceLastUpdate := now.Sub(f.lastRateUpdate)
	if timeSinceLastUpdate < f.adjustmentPeriod {
		return
	}

	actualRate := 0.0
	if timeSinceLastUpdate.Seconds() > 0 {
		actualRate = float64(f.interestsSentInPeriod) / timeSinceLastUpdate.Seconds()
	}

	if len(f.window) == 0 && consumerHasNoSampleObservation(f) {
		interestQ := 0.0
		interestSlope := 0.0
		dataQ := 0.0
		if consumerRateControlMode == ConsumerRateControlQsf {
			currentQ, queueSlope, selectedSignal, qInterest, qInterestSlope, qData, _ := f.computeDominantQsfSignal(now)
			_ = currentQ
			_ = queueSlope
			_ = selectedSignal
			interestQ = qInterest
			interestSlope = qInterestSlope
			dataQ = qData
		}
		rule := consumerNoSampleRuleString()
		f.lastBWUpdateRule = rule
		oldRate := f.currentRate
		f.lastRateUpdate = now
		/*
			log.Debug(consumerTag, "Rate Control Frozen",
				"mode", consumerRateControlModeString(),
				"prefix", f.Prefix,
				"qsfSignal", selectedSignal,
				"qsfAvg", consumerLogFloat1(currentQ),
				"qsfSlope", consumerLogFloat1(queueSlope),
				"interestQsfAvg", consumerLogFloat1(interestQ),
				"interestQsfSlope", consumerLogFloat1(interestSlope),
				"dataQsfAvg", consumerLogFloat1(dataQ),
				"prevTargetRate", consumerLogFloat1(oldRate),
				"actualRatePrevInterval", consumerLogFloat1(actualRate),
				"nextTargetRate", consumerLogFloat1(f.currentRate),
				"estimatedBW", consumerLogFloat1(f.estimatedBandwidth),
				"bwUpdateRule", rule,
				"bwSamples", f.bwSampleCountFromWindow(),
				"action", "frozen-no-qsf-samples")
		*/
		f.writeRateControlCsv(
			now,
			interestQ,
			interestSlope,
			dataQ,
			f.estimatedBandwidth,
			f.bwSampleCountFromWindow(),
			oldRate,
			actualRate,
			f.currentRate,
			"frozen",
		)
		f.interestsSentInPeriod = 0
		return
	}

	if !f.bwCapActive &&
		f.bwBootstrapReady &&
		f.bwCapEligible &&
		f.estimatedBandwidth >= DT_INIT_RATE {
		f.bwCapActive = true
		payloadBytes := f.windowPayloadAvgBytes()
		if payloadBytes <= 0 {
			payloadBytes = float64(datapacket.NewDataPacket().GetSize())
		}
		log.Debug(consumerTag, "DT BW cap activated",
			"prefix", f.Prefix,
			"estimatedBW_Pps", consumerLogFloat1(f.estimatedBandwidth),
			"estimatedBW_Mbps", consumerLogFloat1(ratePpsToMbps(f.estimatedBandwidth, payloadBytes)),
			"initRatePps", consumerLogFloat1(DT_INIT_RATE),
			"initRateMbps", consumerLogFloat1(ratePpsToMbps(DT_INIT_RATE, payloadBytes)))
	}

	switch consumerRateControlMode {
	case ConsumerRateControlQsf:
		f.updateQsfRateControl(now, actualRate)
	default:
		f.updateDelayRateControl(now, actualRate)
	}

	f.interestsSentInPeriod = 0
}

func (f *FlowContext) trimRateControlWindow(now time.Time) {
	cutoff := now.Add(-THROUGHPUT_WINDOW_DUR)
	validWinIdx := len(f.window)
	for i, s := range f.window {
		if !s.Timestamp.Before(cutoff) {
			validWinIdx = i
			break
		}
	}
	if validWinIdx > 0 {
		f.window = f.window[validWinIdx:]
	}
}

func consumerLatestQsfSamples(window []PacketSample) []PacketSample {
	if len(window) <= consumerQsfSampleLimit {
		return window
	}
	return window[len(window)-consumerQsfSampleLimit:]
}

func consumerComputeQsfSignal(now time.Time, samples []PacketSample, selector func(PacketSample) float64) (float64, float64) {
	if len(samples) == 0 {
		return 0, 0
	}

	tau := THROUGHPUT_WINDOW_DUR.Seconds() * consumerQsfRateControlParams.SlopeTauRatio
	if tau <= 0 {
		tau = THROUGHPUT_WINDOW_DUR.Seconds() / 3.0
	}

	var avgQsf float64
	var sumW, sumWT, sumWQ, sumWTT, sumWTQ float64
	for _, sample := range samples {
		qsf := selector(sample)
		avgQsf += qsf

		t := sample.Timestamp.Sub(now).Seconds()
		w := math.Exp(t / tau)
		sumW += w
		sumWT += w * t
		sumWQ += w * qsf
		sumWTT += w * t * t
		sumWTQ += w * t * qsf
	}
	avgQsf /= float64(len(samples))

	denom := sumW*sumWTT - sumWT*sumWT
	if math.Abs(denom) < 1e-9 {
		return avgQsf, 0
	}
	slope := (sumW*sumWTQ - sumWT*sumWQ) / denom
	return avgQsf, slope
}

func (f *FlowContext) observeInterestQsfBias(rawQsf float64) {
	if !DT_USE_QSF_BIAS_REMOVAL {
		return
	}
	if f.interestQsfBiasReady {
		return
	}
	f.interestQsfBiasSum += rawQsf
	f.interestQsfBiasCount++
	f.interestQsfBias = f.interestQsfBiasSum / float64(f.interestQsfBiasCount)
	if f.interestQsfBiasCount >= consumerQsfSampleLimit {
		f.interestQsfBiasReady = true
		log.Debug(consumerTag, "DT interest QSF bias initialized",
			"prefix", f.Prefix,
			"interestQsfBias", consumerLogFloat1(f.interestQsfBias),
			"samples", f.interestQsfBiasCount)
	}
}

func (f *FlowContext) correctedInterestQsf(rawQsf float64) float64 {
	if !DT_USE_QSF_BIAS_REMOVAL {
		return rawQsf
	}
	corrected := rawQsf - f.interestQsfBias
	if corrected < 0 {
		return 0
	}
	return corrected
}

func (f *FlowContext) computeQsfSignalsLocked(now time.Time) (float64, float64, float64, float64) {
	if len(f.window) == 0 {
		if f.lastQsfObservationOk {
			return f.lastInterestQsfAvg, f.lastInterestQsfSlope, f.lastDataQsfAvg, f.lastDataQsfSlope
		}
		return 0, 0, 0, 0
	}

	samples := consumerLatestQsfSamples(f.window)

	interestQ, interestSlope := consumerComputeQsfSignal(now, samples, func(sample PacketSample) float64 {
		return f.correctedInterestQsf(sample.InterestQsf)
	})
	dataQ, dataSlope := consumerComputeQsfSignal(now, samples, func(sample PacketSample) float64 {
		return sample.DataQsf
	})
	f.lastInterestQsfAvg = interestQ
	f.lastInterestQsfSlope = interestSlope
	f.lastDataQsfAvg = dataQ
	f.lastDataQsfSlope = dataSlope
	f.lastQsfObservationOk = true
	return interestQ, interestSlope, dataQ, dataSlope
}

func consumerRateControlModeString() string {
	switch consumerRateControlMode {
	case ConsumerRateControlQsf:
		return "qsf"
	default:
		return "delay"
	}
}

func consumerHasNoSampleObservation(f *FlowContext) bool {
	switch consumerRateControlMode {
	case ConsumerRateControlQsf:
		return f.lastQsfObservationOk
	default:
		return !f.lastRateSampleTime.IsZero()
	}
}

func consumerNoSampleRuleString() string {
	switch consumerRateControlMode {
	case ConsumerRateControlQsf:
		return "no-qsf-sample-retain"
	default:
		return "no-delay-sample-retain"
	}
}

func consumerControlPeriodFromBootstrap(baseRTT float64) time.Duration {
	if DT_USE_STATIC_CONTROL_PERIOD {
		return DT_STATIC_CONTROL_PERIOD
	}
	period := time.Duration(baseRTT * DT_CONTROL_PERIOD_RTT_SCALE * float64(time.Millisecond))
	if period < DT_STATIC_CONTROL_PERIOD {
		period = DT_STATIC_CONTROL_PERIOD
	}
	return period
}

func consumerBwObservationSpacing(period time.Duration) time.Duration {
	if period <= 0 {
		period = DT_STATIC_CONTROL_PERIOD
	}
	spacing := period / 3
	if spacing <= 0 {
		spacing = time.Millisecond
	}
	return spacing
}

func consumerBwTrend(observations []float64) bwTrend {
	if len(observations) < 2 {
		return bwTrendUnknown
	}

	increasing := true
	decreasing := true
	for i := 1; i < len(observations); i++ {
		if observations[i] <= observations[i-1] {
			increasing = false
		}
		if observations[i] >= observations[i-1] {
			decreasing = false
		}
	}
	switch {
	case increasing:
		return bwTrendIncreasing
	case decreasing:
		return bwTrendDecreasing
	default:
		return bwTrendFluctuating
	}
}

func averageFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func consumerQsfActionString(action ConsumerQsfRateControlAction) string {
	switch action {
	case ConsumerQsfActionEmergencyDecrease:
		return "emergency-decrease"
	case ConsumerQsfActionHoldDraining:
		return "hold-draining"
	case ConsumerQsfActionGentleDecrease:
		return "gentle-decrease"
	case ConsumerQsfActionCautiousIncrease:
		return "cautious-increase"
	case ConsumerQsfActionAggressiveProbe:
		return "aggressive-probe"
	case ConsumerQsfActionBWCapped:
		return "bw-capped"
	case ConsumerQsfActionBWBoosted:
		return "bw-boosted"
	case ConsumerQsfActionGlobalMinCapped:
		return "global-min-capped"
	case ConsumerQsfActionGlobalMaxCapped:
		return "global-max-capped"
	default:
		return "unknown"
	}
}

func (f *FlowContext) checkRateControlNoFreshSamples(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.Mode != ModeDT || f.pktsCalibrated < 5 || f.isFinished {
		return
	}

	f.trimRateControlWindow(now)
	if len(f.window) > 0 || f.estimatedBandwidth <= 0 {
		return
	}

	warnEvery := consumerQsfRateControlParams.NoSampleWarnEvery
	if warnEvery <= 0 {
		warnEvery = 500 * time.Millisecond
	}
	if !f.lastNoSampleWarn.IsZero() && now.Sub(f.lastNoSampleWarn) < warnEvery {
		return
	}

	staleFor := time.Duration(0)
	if !f.lastRateSampleTime.IsZero() {
		staleFor = now.Sub(f.lastRateSampleTime)
	}
	lastRule := f.lastBWUpdateRule
	f.lastNoSampleWarn = now
	log.Warn(consumerTag, "DT BW estimate retained without fresh samples",
		"prefix", f.Prefix,
		"mode", consumerRateControlModeString(),
		"retainedBWPps", consumerLogFloat1(f.estimatedBandwidth),
		"lastMeasuredBWPps", consumerLogFloat1(f.lastMeasuredBandwidth),
		"lastUpdateRule", lastRule,
		"staleFor", staleFor)
}

func (f *FlowContext) windowPayloadAvgBytes() float64 {
	if len(f.window) == 0 {
		return 0
	}
	totalBytes := 0
	for _, s := range f.window {
		totalBytes += s.Size
	}
	return float64(totalBytes) / float64(len(f.window))
}

func (f *FlowContext) windowRTTAvg() float64 {
	totalRTT := 0.0
	count := 0
	for _, s := range f.window {
		if !s.RTTValid {
			continue
		}
		totalRTT += s.RTT
		count++
	}
	if count == 0 {
		return 0
	}
	return totalRTT / float64(count)
}

func (f *FlowContext) measuredPPSFromWindow() float64 {
	if len(f.window) == 0 || THROUGHPUT_WINDOW_DUR <= 0 {
		return 0
	}
	return float64(len(f.window)) / THROUGHPUT_WINDOW_DUR.Seconds()
}

func (f *FlowContext) bwSampleCountFromWindow() int {
	return len(f.window)
}

func (f *FlowContext) updateDelayRateControl(receiveTime time.Time, actualRate float64) {
	avgWinRTT := f.windowRTTAvg()
	if avgWinRTT <= 0 {
		avgWinRTT = f.srtt
	}
	if avgWinRTT <= 0 {
		avgWinRTT = f.baseRTT
	}
	measuredPPS := f.lastMeasuredBandwidth
	interestQ, interestSlope := f.computeInterestQsfSignal(receiveTime)
	dataQ, _ := f.computeDataQsfSignal(receiveTime)

	bwUpdateRule := f.lastBWUpdateRule

	targetRate := f.currentRate
	if avgWinRTT < RC_RTT_LOW_THRESH_MULT*f.baseRTT {
		targetRate = f.currentRate * RC_INC_FACTOR
	} else if avgWinRTT >= RC_RTT_HIGH_THRESH_MULT*f.baseRTT {
		targetRate = f.estimatedBandwidth * RC_DEC_FACTOR
	}

	if f.bwCapActive {
		lower := RC_CAP_LOWER_MULT * f.estimatedBandwidth
		if lower < 1.0 {
			lower = 1.0
		}
		upper := RC_CAP_UPPER_MULT * f.estimatedBandwidth
		if upper < DT_INIT_RATE {
			upper = DT_INIT_RATE
		}

		if targetRate < lower {
			targetRate = lower
		}
		if targetRate > upper {
			targetRate = upper
		}
	}
	if targetRate < RC_MIN_RATE {
		targetRate = RC_MIN_RATE
	}

	oldRate := f.currentRate
	f.currentRate = targetRate
	f.lastRateUpdate = receiveTime
	avgPayloadBytes := f.windowPayloadAvgBytes()
	estimatedBWMbps := (f.estimatedBandwidth * avgPayloadBytes * 8.0) / 1e6
	_ = measuredPPS
	_ = bwUpdateRule
	_ = estimatedBWMbps

	/*
		log.Debug(consumerTag, "Rate Adjusted",
			"mode", "delay",
			"prefix", f.Prefix,
			"prevTargetRate", consumerLogFloat1(oldRate),
			"actualRatePrevInterval", consumerLogFloat1(actualRate),
			"nextTargetRate", consumerLogFloat1(f.currentRate),
			"measuredPPS", consumerLogFloat1(measuredPPS),
			"estimatedBW", consumerLogFloat1(f.estimatedBandwidth),
			"estimatedBWMbps", consumerLogFloat1(estimatedBWMbps),
			"bwUpdateRule", bwUpdateRule,
			"baseRTT", consumerLogFloat1(f.baseRTT),
			"latestRTT", consumerLogFloat1(avgWinRTT))
	*/
	f.writeRateControlCsv(
		receiveTime,
		interestQ,
		interestSlope,
		dataQ,
		f.estimatedBandwidth,
		f.bwSampleCountFromWindow(),
		oldRate,
		actualRate,
		f.currentRate,
		"normal",
	)
}

func (f *FlowContext) updateQsfRateControl(receiveTime time.Time, actualRate float64) {
	params := consumerQsfRateControlParams
	currentQ, queueSlope, selectedSignal, interestQ, interestSlope, dataQ, dataSlope := f.computeDominantQsfSignal(receiveTime)
	measuredPPS := f.measuredPPSFromWindow()
	bwUpdateRule := f.lastBWUpdateRule

	minRate := math.Max(params.MinRate, RC_MIN_RATE)
	currentRate := f.currentRate
	if currentRate <= 0 {
		currentRate = minRate
	}

	qthHigh := params.MaxQueueSize * params.QueueBeta
	qthLow := params.MaxQueueSize * params.QueueAlpha
	targetRate := currentRate
	action := ConsumerQsfActionHoldDraining

	if currentQ >= qthHigh {
		if queueSlope > params.SlopeThreshold {
			targetRate = currentRate * params.MDFactor
			action = ConsumerQsfActionEmergencyDecrease
		} else if queueSlope < -params.SlopeThreshold {
			targetRate = currentRate
			action = ConsumerQsfActionHoldDraining
		} else {
			targetRate = currentRate * params.GDFactor
			action = ConsumerQsfActionGentleDecrease
		}
	} else if currentQ >= qthLow {
		targetRate = currentRate
		action = ConsumerQsfActionHoldDraining
	} else {
		if queueSlope > params.SlopeThreshold {
			targetRate = currentRate * params.CIFactor
			action = ConsumerQsfActionCautiousIncrease
		} else {
			targetRate = currentRate * params.RPFactor
			action = ConsumerQsfActionAggressiveProbe
		}
	}

	if f.bwCapActive && f.estimatedBandwidth > 0 {
		upper := f.estimatedBandwidth * params.MaxBWSafetyRatio
		if upper > 0 && targetRate > upper {
			targetRate = upper
			action = ConsumerQsfActionBWCapped
		}

		if currentQ < qthLow {
			lower := f.estimatedBandwidth * params.MinBWSafetyRatio
			if lower > 0 && targetRate < lower {
				targetRate = lower
				action = ConsumerQsfActionBWBoosted
			}
		}
	}

	if targetRate < minRate {
		targetRate = minRate
		action = ConsumerQsfActionGlobalMinCapped
	}
	if params.MaxRate > 0 && targetRate > params.MaxRate {
		targetRate = params.MaxRate
		action = ConsumerQsfActionGlobalMaxCapped
	}

	oldRate := f.currentRate
	f.currentRate = targetRate
	f.lastRateUpdate = receiveTime
	avgPayloadBytes := f.windowPayloadAvgBytes()
	estimatedBWMbps := (f.estimatedBandwidth * avgPayloadBytes * 8.0) / 1e6
	_ = selectedSignal
	_ = interestQ
	_ = interestSlope
	_ = dataSlope
	_ = measuredPPS
	_ = bwUpdateRule
	_ = estimatedBWMbps
	_ = action

	/*
		log.Debug(consumerTag, "Rate Adjusted",
			"mode", "qsf",
			"prefix", f.Prefix,
			"qsfSignal", selectedSignal,
			"qsfAvg", consumerLogFloat1(currentQ),
			"qsfSlope", consumerLogFloat1(queueSlope),
			"interestQsfAvg", consumerLogFloat1(interestQ),
			"interestQsfSlope", consumerLogFloat1(interestSlope),
			"dataQsfAvg", consumerLogFloat1(dataQ),
			"dataQsfSlope", consumerLogFloat1(dataSlope),
			"prevTargetRate", consumerLogFloat1(oldRate),
			"actualRatePrevInterval", consumerLogFloat1(actualRate),
			"nextTargetRate", consumerLogFloat1(f.currentRate),
			"measuredPPS", consumerLogFloat1(measuredPPS),
			"estimatedBW", consumerLogFloat1(f.estimatedBandwidth),
			"estimatedBWMbps", consumerLogFloat1(estimatedBWMbps),
			"bwUpdateRule", bwUpdateRule,
			"action", consumerQsfActionString(action))
	*/
	f.writeRateControlCsv(
		receiveTime,
		currentQ,
		queueSlope,
		dataQ,
		f.estimatedBandwidth,
		f.bwSampleCountFromWindow(),
		oldRate,
		actualRate,
		f.currentRate,
		"normal",
	)
}

func (f *FlowContext) computeInterestQsfSignal(now time.Time) (float64, float64) {
	interestQ, interestSlope, _, _ := f.computeQsfSignalsLocked(now)
	return interestQ, interestSlope
}

func (f *FlowContext) computeDataQsfSignal(now time.Time) (float64, float64) {
	_, _, dataQ, dataSlope := f.computeQsfSignalsLocked(now)
	return dataQ, dataSlope
}

func (f *FlowContext) computeDominantQsfSignal(now time.Time) (float64, float64, string, float64, float64, float64, float64) {
	interestQ, interestSlope, dataQ, dataSlope := f.computeQsfSignalsLocked(now)
	if interestQ >= dataQ {
		return interestQ, interestSlope, "interest", interestQ, interestSlope, dataQ, dataSlope
	}
	return dataQ, dataSlope, "data", interestQ, interestSlope, dataQ, dataSlope
}

func (f *FlowContext) updateQsfBandwidthEstimate(now time.Time, queuePressureQsf float64) string {
	if f.adjustmentPeriod <= 0 {
		return f.lastBWUpdateRule
	}

	measuredPPS := f.measuredPPSFromWindow()
	sampleCount := f.bwSampleCountFromWindow()
	prevBW := f.estimatedBandwidth
	rule := "retain"
	f.lastMeasuredBandwidth = measuredPPS

	switch {
	case sampleCount < consumerBwSampleMin:
		if sampleCount == 0 {
			rule = "no-sample-retain"
		} else if prevBW > 0 {
			rule = "insufficient-sample-retain"
		} else {
			rule = "insufficient-sample-skip"
		}
	case measuredPPS <= 0 && prevBW > 0:
		rule = "no-sample-retain"
	}

	if sampleCount < consumerBwSampleMin || measuredPPS <= 0 {
		f.lastBWUpdateRule = rule
		return rule
	}

	spacing := consumerBwObservationSpacing(f.adjustmentPeriod)
	if !f.lastBwObservationTime.IsZero() && now.Sub(f.lastBwObservationTime) < spacing {
		f.lastBWUpdateRule = "cadence-wait"
		return f.lastBWUpdateRule
	}

	f.lastBwObservationTime = now
	f.bwObservations = append(f.bwObservations, measuredPPS)
	if len(f.bwObservations) > consumerBwObsLimit {
		f.bwObservations = f.bwObservations[len(f.bwObservations)-consumerBwObsLimit:]
	}

	if !f.bwBootstrapReady {
		if len(f.bwObservations) < consumerBwObsLimit {
			f.lastBWUpdateRule = "bootstrap-collecting"
			return f.lastBWUpdateRule
		}

		f.estimatedBandwidth = averageFloat64(f.bwObservations)
		f.bwBootstrapReady = true
		f.lastBWUpdateRule = "bootstrap-ready"
		f.lastRateUpdate = now
		f.interestsSentInPeriod = 0
		log.Debug(consumerTag, "DT BW bootstrap ready",
			"prefix", f.Prefix,
			"estimatedBW", consumerLogFloat1(f.estimatedBandwidth),
			"observations", len(f.bwObservations),
			"observationSpacing", spacing)
		return f.lastBWUpdateRule
	}

	latest := f.bwObservations[len(f.bwObservations)-1]
	avgRecent := averageFloat64(f.bwObservations)
	trend := consumerBwTrend(f.bwObservations)
	qthLow := consumerQsfRateControlParams.MaxQueueSize * consumerQsfRateControlParams.QueueAlpha
	triggerUp := measuredPPS > prevBW
	triggerPressure := queuePressureQsf > qthLow
	candidate := prevBW
	alpha := 0.0

	switch {
	case triggerUp && trend == bwTrendIncreasing:
		candidate = avgRecent
		alpha = consumerBwAlphaUp
		rule = "throughput-high-monotonic"
	case triggerUp:
		candidate = latest
		alpha = consumerBwAlphaSmall
		rule = "throughput-high-fluctuating"
	case triggerPressure && trend == bwTrendDecreasing:
		candidate = latest
		alpha = consumerBwAlphaDown
		rule = "queue-pressure-monotonic"
	case triggerPressure:
		candidate = latest
		alpha = consumerBwAlphaSmall
		rule = "queue-pressure-fluctuating"
	default:
		rule = "retain"
	}

	if alpha > 0 {
		f.estimatedBandwidth = prevBW + alpha*(candidate-prevBW)
		if f.estimatedBandwidth >= DT_INIT_RATE {
			f.bwCapEligible = true
		}
	}
	f.lastBWUpdateRule = rule
	return rule
}

func (f *FlowContext) updateDelayBandwidthEstimate(now time.Time) string {
	if f.adjustmentPeriod <= 0 {
		return f.lastBWUpdateRule
	}

	measuredPPS := f.measuredPPSFromWindow()
	sampleCount := f.bwSampleCountFromWindow()
	prevBW := f.estimatedBandwidth
	rule := "retain"
	f.lastMeasuredBandwidth = measuredPPS

	switch {
	case sampleCount < consumerBwSampleMin:
		if sampleCount == 0 {
			rule = "no-sample-retain"
		} else if prevBW > 0 {
			rule = "insufficient-sample-retain"
		} else {
			rule = "insufficient-sample-skip"
		}
	case measuredPPS <= 0 && prevBW > 0:
		rule = "no-sample-retain"
	}

	if sampleCount < consumerBwSampleMin || measuredPPS <= 0 {
		f.lastBWUpdateRule = rule
		return rule
	}

	spacing := consumerBwObservationSpacing(f.adjustmentPeriod)
	if !f.lastBwObservationTime.IsZero() && now.Sub(f.lastBwObservationTime) < spacing {
		f.lastBWUpdateRule = "cadence-wait"
		return f.lastBWUpdateRule
	}

	f.lastBwObservationTime = now
	f.bwObservations = append(f.bwObservations, measuredPPS)
	if len(f.bwObservations) > consumerBwObsLimit {
		f.bwObservations = f.bwObservations[len(f.bwObservations)-consumerBwObsLimit:]
	}

	if !f.bwBootstrapReady {
		if len(f.bwObservations) < consumerBwObsLimit {
			f.lastBWUpdateRule = "bootstrap-collecting"
			return f.lastBWUpdateRule
		}

		f.estimatedBandwidth = averageFloat64(f.bwObservations)
		f.bwBootstrapReady = true
		f.lastBWUpdateRule = "bootstrap-ready"
		f.lastRateUpdate = now
		f.interestsSentInPeriod = 0
		log.Debug(consumerTag, "DT BW bootstrap ready",
			"prefix", f.Prefix,
			"mode", consumerRateControlModeString(),
			"estimatedBW", consumerLogFloat1(f.estimatedBandwidth),
			"observations", len(f.bwObservations),
			"observationSpacing", spacing)
		return f.lastBWUpdateRule
	}

	latest := f.bwObservations[len(f.bwObservations)-1]
	avgRecent := averageFloat64(f.bwObservations)
	trend := consumerBwTrend(f.bwObservations)
	avgWinRTT := f.windowRTTAvg()
	triggerUp := measuredPPS > prevBW
	triggerDelay := f.baseRTT > 0 && avgWinRTT >= RC_RTT_HIGH_THRESH_MULT*f.baseRTT
	candidate := prevBW
	alpha := 0.0

	switch {
	case triggerUp && trend == bwTrendIncreasing:
		candidate = avgRecent
		alpha = consumerBwAlphaUp
		rule = "throughput-high-monotonic"
	case triggerUp:
		candidate = latest
		alpha = consumerBwAlphaSmall
		rule = "throughput-high-fluctuating"
	case triggerDelay && trend == bwTrendDecreasing:
		candidate = latest
		alpha = consumerBwAlphaDown
		rule = "delay-pressure-monotonic"
	case triggerDelay:
		candidate = latest
		alpha = consumerBwAlphaSmall
		rule = "delay-pressure-fluctuating"
	default:
		rule = "retain"
	}

	if alpha > 0 {
		f.estimatedBandwidth = prevBW + alpha*(candidate-prevBW)
		if f.estimatedBandwidth >= DT_INIT_RATE {
			f.bwCapEligible = true
		}
	}
	f.lastBWUpdateRule = rule
	return rule
}

func (f *FlowContext) GetFinalStats() FlowFinalStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	duration := consumerDurationSeconds(f.startTime, f.endTime)
	throughputMbps := 0.0
	if duration > 0 {
		throughputMbps = (float64(f.totalBytes) * 8.0) / (duration * 1e6)
	}
	p95 := consumerDurationSeconds(f.startTime, f.p95Time)
	p99 := consumerDurationSeconds(f.startTime, f.p99Time)
	p100 := consumerDurationSeconds(f.startTime, f.p100Time)
	complete := f.isFinished && f.pktsReceivedTotal >= MAX_PACKETS
	if !complete {
		p100 = 0
	}
	missingPackets := MAX_PACKETS - f.pktsReceivedTotal
	if missingPackets < 0 {
		missingPackets = 0
	}
	return FlowFinalStats{
		ThroughputMbps:  throughputMbps,
		TotalBytes:      f.totalBytes,
		Duration:        duration,
		FCT95:           p95,
		FCT99:           p99,
		FCT100:          p100,
		Complete:        complete,
		ReceivedPackets: f.pktsReceivedTotal,
		ExpectedPackets: MAX_PACKETS,
		MissingPackets:  missingPackets,
		RTOExpirations:  f.timeoutEvents,
		RateBackoffs:    f.timeoutBackoffs,
		MaxInflight:     f.maxInflight,
		FinalCwnd:       f.congestionWindow,
	}
}

func closeNodeThroughputCsvSinks() {
	for _, sink := range nodeThroughputSinks {
		if sink == nil {
			continue
		}
		sink.mu.Lock()
		if sink.writer != nil {
			sink.writer.Flush()
		}
		if sink.file != nil {
			_ = sink.file.Close()
			sink.file = nil
		}
		sink.mu.Unlock()
	}
}

func writeNodeThroughputCsvRow(now time.Time, flowContexts []*FlowContext, throughputs map[string]float64, sampleInterval time.Duration) {
	sink := getNodeThroughputCsvSink(sampleInterval)
	if sink == nil {
		return
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if sink.writer == nil {
		if err := os.MkdirAll(filepath.Dir(sink.path), 0o755); err != nil {
			log.Warn(consumerTag, "Failed to create node throughput CSV directory", "path", sink.path, "err", err)
			return
		}
		file, err := os.OpenFile(sink.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			log.Warn(consumerTag, "Failed to open node throughput CSV", "path", sink.path, "err", err)
			return
		}
		sink.file = file
		sink.writer = csv.NewWriter(file)
	}

	if !sink.headerWritten {
		header := []string{"time_ms"}
		for _, flowCtx := range flowContexts {
			header = append(header, flowPrefixCsvLabel(flowCtx.Prefix)+"_throughput_mbps")
		}
		if err := sink.writer.Write(header); err != nil {
			log.Warn(consumerTag, "Failed to write node throughput CSV header", "path", sink.path, "err", err)
			return
		}
		sink.headerWritten = true
	}

	row := []string{consumerCsvFloat(consumerElapsedMs(now))}
	for _, flowCtx := range flowContexts {
		row = append(row, consumerCsvFloat(throughputs[flowCtx.Prefix]))
	}
	if err := sink.writer.Write(row); err != nil {
		log.Warn(consumerTag, "Failed to write node throughput CSV row", "path", sink.path, "err", err)
		return
	}
	sink.writer.Flush()
	if err := sink.writer.Error(); err != nil {
		log.Warn(consumerTag, "Failed to flush node throughput CSV", "path", sink.path, "err", err)
	}
}

func runNodeThroughputSampler(flowContexts []*FlowContext, stopCh <-chan struct{}, sampleInterval time.Duration) {
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

	prevBytes := make(map[string]int64, len(flowContexts))
	prevStart := make(map[string]time.Time, len(flowContexts))
	prevSampleTime := time.Time{}

	for {
		select {
		case <-stopCh:
			return
		case tickNow := <-ticker.C:
			throughputs := make(map[string]float64, len(flowContexts))
			elapsed := 0.0
			if !prevSampleTime.IsZero() {
				elapsed = tickNow.Sub(prevSampleTime).Seconds()
			}

			for _, flowCtx := range flowContexts {
				snapshot := flowCtx.SnapshotProgress()
				throughput := 0.0
				if snapshot.Mode == ModeDT && !snapshot.StartTime.IsZero() {
					prevFlowStart := prevStart[snapshot.Prefix]
					prevFlowBytes := prevBytes[snapshot.Prefix]
					if !prevSampleTime.IsZero() && prevFlowStart.Equal(snapshot.StartTime) && elapsed > 0 {
						deltaBytes := snapshot.TotalBytes - prevFlowBytes
						if deltaBytes > 0 {
							throughput = (float64(deltaBytes) * 8.0) / (elapsed * 1e6)
						}
					}
				}
				prevBytes[snapshot.Prefix] = snapshot.TotalBytes
				prevStart[snapshot.Prefix] = snapshot.StartTime
				throughputs[snapshot.Prefix] = throughput
			}

			writeNodeThroughputCsvRow(tickNow, flowContexts, throughputs, sampleInterval)
			prevSampleTime = tickNow
		}
	}
}

// --- Global Coordination ---

func checkAllPdFinished() {
	pdMutex.Lock()
	pdFinishedCount++
	allDone := pdFinishedCount == pdTotalFlows
	pdMutex.Unlock()

	if allDone {
		log.Info(consumerTag, "All flows finished PD. Preparing for DT...")
		close(startDtCh)
	}
}

func startDataTransmission(f *FlowContext) {
	delay := time.Duration(0)

	if consumerIndex%2 == 0 {
		log.Info(consumerTag, "DT Start", "flow", f.Prefix, "type", "Even/Immediate")
	} else {
		delay = oddConsumerDtDelay
		log.Info(consumerTag, "DT Start", "flow", f.Prefix, "type", "Odd/Delayed", "delay", delay)
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	f.mu.Lock()
	f.Mode = ModeDT
	f.initDTState()
	f.mu.Unlock()

	select {
	case f.resetSignal <- struct{}{}:
	default:
	}
}

// --- Statistics ---

type Statistics struct {
	mu              sync.Mutex
	totalSent       int
	dataReceived    int
	nackReceived    int
	timeoutReceived int
	retransmissions int
}

func (s *Statistics) IncrementSent()    { s.mu.Lock(); s.totalSent++; s.mu.Unlock() }
func (s *Statistics) IncrementData()    { s.mu.Lock(); s.dataReceived++; s.mu.Unlock() }
func (s *Statistics) IncrementNack()    { s.mu.Lock(); s.nackReceived++; s.mu.Unlock() }
func (s *Statistics) IncrementTimeout() { s.mu.Lock(); s.timeoutReceived++; s.mu.Unlock() }
func (s *Statistics) IncrementRetx()    { s.mu.Lock(); s.retransmissions++; s.mu.Unlock() }

func (s *Statistics) Print() {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Debug(consumerTag, "=== Final Statistics ===",
		"totalSent", s.totalSent,
		"dataReceived", s.dataReceived,
		"nackReceived", s.nackReceived,
		"timeouts", s.timeoutReceived,
		"retransmissions", s.retransmissions)
}

// --- Heap-based Retransmission Controller ---

type PendingPacket struct {
	Name            string
	Prefix          string
	Expiration      time.Time
	SequenceNum     int
	Generation      uint64
	Attempt         int
	RTOBackoffLevel int
	Timeout         time.Duration
	FlowCtx         *FlowContext
	Index           int
}

type ActiveAttempt struct {
	SequenceNum int
	Generation  uint64
	// Attempt identifies each send; RTOBackoffLevel excludes fast repair sends.
	Attempt             int
	RTOBackoffLevel     int
	Timeout             time.Duration
	Mode                TransmissionMode
	FlowCtx             *FlowContext
	FastRepairTriggered bool
	FastRepairQueued    bool
}

type PacketHeap []*PendingPacket

func (h PacketHeap) Len() int           { return len(h) }
func (h PacketHeap) Less(i, j int) bool { return h[i].Expiration.Before(h[j].Expiration) }
func (h PacketHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].Index = i; h[j].Index = j }
func (h *PacketHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*PendingPacket)
	item.Index = n
	*h = append(*h, item)
}
func (h *PacketHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	*h = old[0 : n-1]
	return item
}

type RetransmissionController struct {
	mu             sync.Mutex
	pq             PacketHeap
	active         map[string]ActiveAttempt
	nextGeneration uint64
	app            ndn.Engine
	stats          *Statistics
	stopCh         chan struct{}
	wakeupCh       chan struct{}
}

func NewRetransmissionController(app ndn.Engine, stats *Statistics) *RetransmissionController {
	rc := &RetransmissionController{
		pq:       make(PacketHeap, 0),
		active:   make(map[string]ActiveAttempt),
		app:      app,
		stats:    stats,
		stopCh:   make(chan struct{}),
		wakeupCh: make(chan struct{}, 1),
	}
	heap.Init(&rc.pq)
	return rc
}

func retransmissionJitter(name string, generation uint64) float64 {
	// A stable mix avoids synchronized retransmission bursts while keeping tests reproducible.
	mixed := generation ^ 1469598103934665603
	for i := 0; i < len(name); i++ {
		mixed ^= uint64(name[i])
		mixed *= 1099511628211
	}
	unit := float64(mixed%20001) / 20000.0
	return (2*unit - 1) * RTO_JITTER_RATIO
}

func retransmissionTimeout(baseRTO float64, backoffLevel int, name string, generation uint64) time.Duration {
	if backoffLevel < 0 {
		backoffLevel = 0
	}
	// Reserve headroom for positive jitter so retries at the cap do not all
	// collapse to exactly MAX_RTO.
	maxBaseRTO := MAX_RTO / (1 + RTO_JITTER_RATIO)
	timeoutMs := math.Min(math.Max(baseRTO, MIN_RTO), maxBaseRTO)
	for retry := 0; retry < backoffLevel && timeoutMs < maxBaseRTO; retry++ {
		timeoutMs = math.Min(timeoutMs*2, maxBaseRTO)
	}
	timeoutMs *= 1 + retransmissionJitter(name, generation)
	if timeoutMs < MIN_RTO {
		timeoutMs = MIN_RTO
	}
	if timeoutMs > MAX_RTO {
		timeoutMs = MAX_RTO
	}
	return time.Duration(timeoutMs * float64(time.Millisecond))
}

func (rc *RetransmissionController) Add(
	name string,
	prefix string,
	seq int,
	flowCtx *FlowContext,
	mode TransmissionMode,
) (ActiveAttempt, bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	baseRTO, pending := flowCtx.GetRTOIfPending(name, seq)
	if !pending {
		return ActiveAttempt{}, false
	}
	attempt := 1
	rtoBackoffLevel := 0
	isFastRepair := false
	if current, exists := rc.active[name]; exists && current.SequenceNum == seq {
		attempt = current.Attempt + 1
		isFastRepair = current.FastRepairQueued
		rtoBackoffLevel = current.RTOBackoffLevel
		// A fast repair changes generation identity but is not evidence that the
		// current recovery timer expired.
		if !isFastRepair {
			rtoBackoffLevel++
		}
	}
	rc.nextGeneration++
	generation := rc.nextGeneration
	timeout := time.Duration(baseRTO * float64(time.Millisecond))
	if mode == ModeDT {
		timeout = retransmissionTimeout(baseRTO, rtoBackoffLevel, name, generation)
	}
	state := ActiveAttempt{
		SequenceNum:     seq,
		Generation:      generation,
		Attempt:         attempt,
		RTOBackoffLevel: rtoBackoffLevel,
		Timeout:         timeout,
		Mode:            mode,
		FlowCtx:         flowCtx,
	}
	pkt := &PendingPacket{
		Name:            name,
		Prefix:          prefix,
		Expiration:      time.Now().Add(timeout),
		SequenceNum:     seq,
		Generation:      generation,
		Attempt:         attempt,
		RTOBackoffLevel: rtoBackoffLevel,
		Timeout:         timeout,
		FlowCtx:         flowCtx,
	}
	rc.active[name] = state
	// Any retransmission queued before this generation was installed belongs to
	// the previous attempt. Remove its marker while rc.mu prevents an old
	// callback from queueing another stale retry concurrently.
	flowCtx.installShadowAttemptAndClearQueued(seq, generation, isFastRepair)
	heap.Push(&rc.pq, pkt)
	select {
	case rc.wakeupCh <- struct{}{}:
	default:
	}
	return state, true
}

func (rc *RetransmissionController) IsCurrent(name string, generation uint64) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	current, exists := rc.active[name]
	return exists && current.Generation == generation
}

func (rc *RetransmissionController) RemoveIfCurrent(name string, generation uint64) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	current, exists := rc.active[name]
	if !exists || current.Generation != generation {
		return false
	}
	delete(rc.active, name)
	return true
}

func (rc *RetransmissionController) Complete(name string) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if _, exists := rc.active[name]; !exists {
		return false
	}
	delete(rc.active, name)
	return true
}

func (rc *RetransmissionController) QueueRetransmissionIfCurrent(name string, generation uint64) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	current, exists := rc.active[name]
	if !exists || current.Generation != generation || current.FlowCtx == nil {
		return false
	}
	return current.FlowCtx.QueueRetransmission(current.SequenceNum)
}

func (rc *RetransmissionController) QueueShadowFastRepairIfCurrent(
	name string,
	candidate shadowFastRepairCandidate,
	flowCtx *FlowContext,
) shadowFastRepairQueueOutcome {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	current, exists := rc.active[name]
	if !exists || current.Generation != candidate.Generation ||
		current.SequenceNum != candidate.Sequence || current.FlowCtx != flowCtx ||
		current.Mode != ModeDT {
		flowCtx.recordShadowFastRepairOutcome(candidate, shadowFastRepairQueueStale)
		return shadowFastRepairQueueStale
	}
	if current.FastRepairTriggered {
		flowCtx.recordShadowFastRepairOutcome(
			candidate, shadowFastRepairQueueAlreadyTriggered,
		)
		return shadowFastRepairQueueAlreadyTriggered
	}

	current.FastRepairTriggered = true
	outcome := flowCtx.queueShadowFastRepair(candidate)
	current.FastRepairQueued = outcome == shadowFastRepairQueueQueued
	rc.active[name] = current
	return outcome
}

// StartScheduler processes retransmission deadlines until the controller stops.
func (rc *RetransmissionController) StartScheduler() {
	for {
		var waitDuration time.Duration
		rc.mu.Lock()
		if rc.pq.Len() == 0 {
			waitDuration = 10 * time.Second
		} else {
			now := time.Now()
			top := rc.pq[0]
			if now.After(top.Expiration) {
				// Packet has expired
				item := heap.Pop(&rc.pq).(*PendingPacket)

				// Only the newest send attempt may schedule another retransmission.
				if current, exists := rc.active[item.Name]; exists && current.Generation == item.Generation {

					// --- DT MODE: Retransmit ---
					if current.Mode == ModeDT {
						// The current generation reached its application RTO even if a
						// retransmission is already queued by another signal (for example,
						// a Nack). Shadow classification must not depend on whether this
						// QueueRetransmission call inserts a second queue entry.
						item.FlowCtx.RecordShadowRTO(item.SequenceNum, now)
						queued := item.FlowCtx.QueueRetransmission(item.SequenceNum)
						newRate := item.FlowCtx.GetRate()
						currentRTO := 0.0
						backedOff := false
						if queued {
							newRate, currentRTO, backedOff = item.FlowCtx.OnDTRTO(now, item.Timeout)
						}
						log.Debug(consumerTag, "DT: RTO Expired - Queueing retransmission",
							"name", item.Name,
							"generation", item.Generation,
							"attempt", item.Attempt,
							"rtoBackoffLevel", item.RTOBackoffLevel,
							"rto", item.Timeout,
							"queued", queued,
							"rateBackoff", backedOff,
							"nextRatePps", consumerLogFloat1(newRate),
							"currentRtoMs", consumerLogFloat1(currentRTO))
						rc.mu.Unlock()
						continue
					} else {
						// --- PD MODE: Handle Timeout Logic (No Retransmit) ---
						log.Debug(consumerTag, "PD: RTO Expired - Handling Logic", "name", item.Name)

						// Remove from active tracking, we treat it as lost/timed out
						delete(rc.active, item.Name)
						rc.mu.Unlock()

						rc.stats.IncrementTimeout()
						// Call the FlowContext's OnTimeout to handle counters/state
						item.FlowCtx.OnTimeout(item.Name)
						continue
					}
				}
				// Item was already removed or stale, just drop
				rc.mu.Unlock()
				continue
			} else {
				waitDuration = top.Expiration.Sub(now)
			}
		}
		rc.mu.Unlock()
		select {
		case <-rc.stopCh:
			return
		case <-rc.wakeupCh:
			continue
		case <-time.After(waitDuration):
			continue
		}
	}
}

func (rc *RetransmissionController) Stop() { close(rc.stopCh) }

var globalRetxController *RetransmissionController

// --- Sending Logic ---

func sendSingleInterest(app ndn.Engine, stats *Statistics, sequenceNum int, flowCtx *FlowContext, isRetransmission bool) {
	name := flowCtx.baseName.Append(enc.NewStringComponent(0x08, "seq-"+strconv.Itoa(sequenceNum)))

	intCfg := &ndn.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(INTEREST_LIFETIME),
		Nonce:       utils.ConvertNonce(app.Timer().Nonce()),
	}

	interest, err := app.Spec().MakeInterest(name, intCfg, nil, nil)
	if err != nil {
		return
	}

	interestName := interest.FinalName.String()
	sendTime := time.Now()
	sendMode, recorded := flowCtx.RecordInterestSend(interestName, sequenceNum, sendTime, isRetransmission)
	if !recorded {
		return
	}

	// Arm the application RTO before Express so every callback captures its own generation.
	attempt, pending := globalRetxController.Add(interestName, flowCtx.Prefix, sequenceNum, flowCtx, sendMode)
	if !pending {
		return
	}
	if !isRetransmission {
		stats.IncrementSent()
	} else {
		stats.IncrementRetx()
	}

	if sequenceNum%200 == 0 && !isRetransmission {
		currentRate := flowCtx.GetRate()
		log.Debug(consumerTag, "Interest Sent",
			"name", interestName,
			"rate", currentRate,
			"isRetx", isRetransmission)
	}

	err = app.Express(interest,
		func(args ndn.ExpressCallbackArgs) {
			receiveTime := time.Now()
			switch args.Result {
			case ndn.InterestResultNack:
				stats.IncrementNack()
				if attempt.Mode == ModeDT {
					queued := globalRetxController.QueueRetransmissionIfCurrent(interestName, attempt.Generation)
					log.Debug(consumerTag, "DT Nack - Queueing retransmission",
						"name", interestName,
						"generation", attempt.Generation,
						"attempt", attempt.Attempt,
						"rtoBackoffLevel", attempt.RTOBackoffLevel,
						"queued", queued,
						"current", globalRetxController.IsCurrent(interestName, attempt.Generation))
				} else if globalRetxController.RemoveIfCurrent(interestName, attempt.Generation) {
					flowCtx.OnNack(interestName)
				}

			case ndn.InterestResultTimeout:
				// The application RTO owns logical retransmission. This per-attempt
				// callback must never remove a newer generation for the same name.
				log.Debug(consumerTag, "NDN Framework Timeout (Lifetime reached)",
					"name", interestName,
					"generation", attempt.Generation,
					"attempt", attempt.Attempt,
					"rtoBackoffLevel", attempt.RTOBackoffLevel,
					"current", globalRetxController.IsCurrent(interestName, attempt.Generation))

			case ndn.InterestCancelled:
				if attempt.Mode == ModeDT {
					queued := globalRetxController.QueueRetransmissionIfCurrent(interestName, attempt.Generation)
					log.Debug(consumerTag, "Current Interest cancelled - Queueing retransmission",
						"name", interestName,
						"generation", attempt.Generation,
						"attempt", attempt.Attempt,
						"rtoBackoffLevel", attempt.RTOBackoffLevel,
						"queued", queued)
				} else if globalRetxController.RemoveIfCurrent(interestName, attempt.Generation) {
					flowCtx.OnNack(interestName)
				}

			case ndn.InterestResultData:
				data := args.Data
				content := data.Content().Join()
				dataName := data.Name().String()
				if flowCtx.OnData(content, interestName, receiveTime) {
					// Valid Data completes the logical name regardless of which attempt returned it.
					globalRetxController.Complete(interestName)
					if dataName != interestName {
						log.Debug(consumerTag, "Data name differs from Interest name",
							"interestName", interestName, "dataName", dataName)
						globalRetxController.Complete(dataName)
					}
					stats.IncrementData()
				}
			}
		})
	if err != nil {
		if attempt.Mode == ModeDT {
			queued := globalRetxController.QueueRetransmissionIfCurrent(interestName, attempt.Generation)
			log.Warn(consumerTag, "Interest send failed - Queueing retransmission",
				"name", interestName,
				"generation", attempt.Generation,
				"attempt", attempt.Attempt,
				"rtoBackoffLevel", attempt.RTOBackoffLevel,
				"queued", queued,
				"err", err)
		} else if globalRetxController.RemoveIfCurrent(interestName, attempt.Generation) {
			flowCtx.OnNack(interestName)
		}
	}
}

// --- Main Flow Loop ---

// runFlowRateControlLoop isolates per-flow rate control from the send/retransmit
// loop so bursts of local work do not directly delay control execution.
func runFlowRateControlLoop(flowCtx *FlowContext, stopCh <-chan struct{}) {
	ticker := time.NewTicker(TICKER_INTERVAL)
	defer ticker.Stop()
	nextShadowScan := time.Time{}

	for {
		select {
		case <-stopCh:
			return
		case tickNow := <-ticker.C:
			if nextShadowScan.IsZero() || !tickNow.Before(nextShadowScan) {
				flowCtx.runShadowLossScan(tickNow, globalRetxController)
				nextShadowScan = tickNow.Add(shadowLossScanInterval)
			}
			flowCtx.checkRateControlNoFreshSamples(tickNow)
			flowCtx.runPeriodicRateControl(tickNow)
		}
	}
}

func runFlow(app ndn.Engine, stats *Statistics, flowCtx *FlowContext, wg *sync.WaitGroup) {
	defer wg.Done()
	totalTimer := time.NewTimer(TOTAL_DURATION)
	defer totalTimer.Stop()
	ticker := time.NewTicker(TICKER_INTERVAL)
	defer ticker.Stop()
	controlStopCh := make(chan struct{})
	defer close(controlStopCh)
	go runFlowRateControlLoop(flowCtx, controlStopCh)

	tokens := 0.0
	sequenceNum := 0
	lastTokenRefill := time.Now()

	log.Info(consumerTag, "Starting flow", "target", flowCtx.Prefix, "mode", flowCtx.Mode)

	for {
		select {
		case <-totalTimer.C:
			return
		case <-flowCtx.doneCh:
			return
		case <-flowCtx.resetSignal:
			log.Info(consumerTag, "Flow resetting for DT", "prefix", flowCtx.Prefix)
			if !totalTimer.Stop() {
				select {
				case <-totalTimer.C:
				default:
				}
			}
			totalTimer.Reset(TOTAL_DURATION)
			tokens = 0.0
			sequenceNum = 0
			lastTokenRefill = time.Now()

		case tickNow := <-ticker.C:
			// PD Logic: Pause Check
			if flowCtx.Mode == ModePD {
				flowCtx.mu.Lock()
				if flowCtx.StopPD {
					flowCtx.mu.Unlock()
					lastTokenRefill = tickNow
					continue
				}
				shouldSend := false
				if flowCtx.RehearsalCount > 0 {
					flowCtx.RehearsalCount--
					shouldSend = true
				} else {
					if !flowCtx.IsPaused {
						shouldSend = true
					}
				}
				flowCtx.mu.Unlock()

				if !shouldSend {
					lastTokenRefill = tickNow
					continue
				}
			}

			currentRate := flowCtx.GetRate()
			elapsed := tickNow.Sub(lastTokenRefill)
			if elapsed < 0 {
				elapsed = 0
			}
			tokens += currentRate * elapsed.Seconds()
			// Do not convert scheduler delay into a large send burst. This pacing
			// bound keeps one flow from consuming a long-idle token balance at once.
			maxBurstTokens := math.Max(1.0, currentRate*DT_SEND_BURST_INTERVAL.Seconds())
			if tokens > maxBurstTokens {
				tokens = maxBurstTokens
			}
			lastTokenRefill = tickNow

			for tokens >= 1.0 {
				if flowCtx.Mode == ModeDT {
					hasRetransmission := flowCtx.HasQueuedRetransmission()
					if !hasRetransmission && sequenceNum >= MAX_PACKETS {
						tokens = 0
						break
					}
					if !hasRetransmission && !flowCtx.CanSendNewDT() {
						break
					}
					if hasRetransmission {
						retxSeq, ok := flowCtx.PopRetransmission()
						if !ok {
							continue
						}
						tokens -= 1.0
						sendSingleInterest(app, stats, retxSeq, flowCtx, true)
						continue
					}
				}

				if sequenceNum >= MAX_PACKETS {
					tokens = 0
					if flowCtx.Mode == ModePD {
						log.Warn(consumerTag, "PD: Max Packets Reached", "prefix", flowCtx.Prefix)
					}
					break
				}

				sequenceNum++
				tokens -= 1.0
				sendSingleInterest(app, stats, sequenceNum, flowCtx, false)
			}
		}
	}
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--print-rto-config" {
		fmt.Printf(
			"rtoOuterMultiplier=%d rtoFormula=%s minRtoMs=%.0f maxRtoMs=%.0f fastRepairAdvancesRtoBackoff=false\n",
			rtoOuterMultiplier,
			RTO_ESTIMATOR_FORMULA,
			MIN_RTO,
			MAX_RTO,
		)
		return
	}
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <node_name> <producer_range> <initialMode>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  initialMode must be either \"ModePD\" or \"ModeDT\"\n")
		os.Exit(1)
	}

	nodeName := os.Args[1]
	rangeStr := os.Args[2]
	modeStr := os.Args[3]
	consumerNodeName = nodeName

	// Parse initial mode from command line argument
	switch modeStr {
	case "ModePD":
		initialMode = ModePD
	case "ModeDT":
		initialMode = ModeDT
	default:
		fmt.Fprintf(os.Stderr, "Error: invalid initialMode \"%s\". Must be \"ModePD\" or \"ModeDT\"\n", modeStr)
		os.Exit(1)
	}

	if idx, err := parseConsumerIndex(nodeName); err == nil {
		consumerIndex = idx
	} else {
		log.Fatal(consumerTag, "Invalid consumer node name", "nodeName", nodeName, "err", err)
		return
	}

	targetPrefixes, err := parseProducerRange(rangeStr)
	if err != nil {
		log.Fatal(consumerTag, "Failed to parse producer range", "err", err)
		return
	}

	consumerRateControlMode = selectConsumerRateControlMode()
	applyAdaptiveDtTunables(len(targetPrefixes))

	log.Default().SetLevel(log.LevelInfo)

	os.Setenv("NDN_CLIENT_TRANSPORT", "tcp://127.0.0.1:6363")

	app := engine.NewBasicEngine(engine.NewDefaultFace())
	err = app.Start()
	if err != nil {
		log.Fatal(consumerTag, "Unable to start engine", "err", err)
		return
	}
	defer app.Stop()

	stats := &Statistics{}
	globalRetxController = NewRetransmissionController(app, stats)
	go globalRetxController.StartScheduler()
	defer globalRetxController.Stop()

	log.Info(consumerTag, "Starting Optimized NDN Consumer",
		"nodeName", nodeName,
		"index", consumerIndex,
		"targets", len(targetPrefixes),
		"initialMode", initialMode,
		"rateControlMode", consumerRateControlModeString(),
		"rtoOuterMultiplier", rtoOuterMultiplier,
		"rtoFormula", RTO_ESTIMATOR_FORMULA,
		"deploymentMode", os.Getenv("MARS_DEPLOYMENT_MODE"))

	pdTotalFlows = len(targetPrefixes)
	startDtCh = make(chan struct{})

	var wg sync.WaitGroup
	startTime := time.Now()
	flowContexts := make([]*FlowContext, 0, len(targetPrefixes))

	for _, prefix := range targetPrefixes {
		wg.Add(1)
		flowCtx := NewFlowContext(prefix)
		flowContexts = append(flowContexts, flowCtx)
		go runFlow(app, stats, flowCtx, &wg)
	}

	throughputStopCh := make(chan struct{})
	go runNodeThroughputSampler(flowContexts, throughputStopCh, consumerThroughputSampleInterval)
	go runNodeThroughputSampler(flowContexts, throughputStopCh, consumerThroughputSampleInterval100)

	go func() {
		<-startDtCh
		for _, f := range flowContexts {
			go startDataTransmission(f)
		}
	}()

	wg.Wait()
	close(throughputStopCh)
	closeNodeThroughputCsvSinks()
	elapsed := time.Since(startTime)
	log.Debug(consumerTag, "All flows stopped", "elapsedTime", elapsed)

	time.Sleep(1 * time.Second)
	stats.Print()

	if initialMode == ModeDT || flowContexts[0].Mode == ModeDT {
		log.Debug(consumerTag, "=== Final Per-Flow Statistics (DT) ===")
		for _, flowCtx := range flowContexts {
			finalStats := flowCtx.GetFinalStats()
			log.Info(consumerTag, "Flow Summary",
				"flow", flowCtx.Prefix,
				"complete", finalStats.Complete,
				"receivedPackets", finalStats.ReceivedPackets,
				"expectedPackets", finalStats.ExpectedPackets,
				"missingPackets", finalStats.MissingPackets,
				"rtoExpirations", finalStats.RTOExpirations,
				"rateBackoffs", finalStats.RateBackoffs,
				"maxInflight", finalStats.MaxInflight,
				"finalCwnd", consumerLogFloat1(finalStats.FinalCwnd),
				"totalBytes", finalStats.TotalBytes,
				"avgMbps", finalStats.ThroughputMbps,
				"duration", finalStats.Duration,
				"fct95", finalStats.FCT95,
				"fct99", finalStats.FCT99,
				"fct100", finalStats.FCT100,
				"rtoOuterMultiplier", rtoOuterMultiplier)
			for _, shadowStats := range flowCtx.GetShadowLossSummaries(time.Now()) {
				log.Debug(consumerTag, "Shadow Loss Summary",
					"flow", flowCtx.Prefix,
					"behavioralRetransmission", true,
					"ageFloorMs", shadowStats.AgeFloor.Milliseconds(),
					"laterAckThreshold", shadowStats.LaterACKThreshold,
					"shadowSuspects", shadowStats.ShadowSuspects,
					"resolvedBeforeRTO", shadowStats.ResolvedBeforeRTO,
					"reachedRTO", shadowStats.ReachedRTO,
					"activeAtEnd", shadowStats.ActiveAtEnd,
					"firstAttemptRTOs", shadowStats.FirstAttemptRTOs,
					"leadToRTOTotalMs", durationMilliseconds(shadowStats.LeadToRTOTotal),
					"avgLeadToRTOMs", durationMilliseconds(shadowStats.AvgLeadToRTO),
					"suspectInflightTotalMs", durationMilliseconds(shadowStats.SuspectInflightTotal),
					"suspectInflightAvgMs", durationMilliseconds(shadowStats.SuspectInflightAvg))
			}
			fastRepairStats := flowCtx.GetShadowFastRepairSummary()
			log.Debug(consumerTag, "Shadow Fast Repair Summary",
				"flow", flowCtx.Prefix,
				"behavioralRetransmission", true,
				"ageFloorMs", fastRepairStats.AgeFloor.Milliseconds(),
				"laterAckThreshold", fastRepairStats.LaterACKThreshold,
				"maxPerGeneration", 1,
				"usesUnifiedPacing", true,
				"rateBackoff", false,
				"cwndBackoff", false,
				"fastRepairAdvancesRtoBackoff", false,
				"rtoOuterMultiplier", rtoOuterMultiplier,
				"triggers", fastRepairStats.Triggers,
				"queued", fastRepairStats.Queued,
				"alreadyQueued", fastRepairStats.AlreadyQueued,
				"alreadyTriggered", fastRepairStats.AlreadyTriggered,
				"stale", fastRepairStats.Stale,
				"sent", fastRepairStats.Sent,
				"resolved", fastRepairStats.Resolved,
				"resolvedAfterSend", fastRepairStats.ResolvedAfterSend)
		}
	}
}

// Helpers

func parseConsumerIndex(nodeName string) (int, error) {
	if !strings.HasPrefix(nodeName, "con") {
		return 0, fmt.Errorf("must match con<index> or con<index>app")
	}
	idxStr := strings.TrimPrefix(nodeName, "con")
	if strings.HasSuffix(idxStr, "app") {
		idxStr = strings.TrimSuffix(idxStr, "app")
	}
	if idxStr == "" {
		return 0, fmt.Errorf("missing consumer index")
	}
	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 0 {
		return 0, fmt.Errorf("invalid consumer index %q", idxStr)
	}
	return idx, nil
}

func extractSeqNum(nameStr string) int {
	parts := strings.Split(nameStr, "/")
	if len(parts) == 0 {
		return -1
	}
	lastPart := parts[len(parts)-1]
	if !strings.HasPrefix(lastPart, "seq-") {
		return -1
	}
	seqStr := strings.TrimPrefix(lastPart, "seq-")
	seqNum, err := strconv.Atoi(seqStr)
	if err != nil {
		return -1
	}
	return seqNum
}

func parseProducerRange(rangeStr string) ([]string, error) {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid format")
	}
	start, err1 := strconv.Atoi(parts[0])
	end, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return nil, fmt.Errorf("invalid numbers")
	}
	var prefixes []string
	for i := start; i <= end; i++ {
		prefixes = append(prefixes, fmt.Sprintf("/pro%dapp", i))
	}
	return prefixes, nil
}
