package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lucas-clemente/quic-go"
)

const mpquicVersion quic.VersionNumber = 512
const additionalPathCongestionControl = "olia"

func main() {
	port := flag.String("port", "6121", "Listen port")
	enableMultipath := flag.Bool("multipath", true, "Enable multipath support")
	certFile := flag.String("cert", "cert.pem", "Path to TLS certificate")
	keyFile := flag.String("key", "key.pem", "Path to TLS key")
	csvPath := flag.String("csv", "", "Path to per-flow throughput CSV")
	csvIntervals := flag.String("csv-intervals", "50,100", "Comma-separated throughput sampling intervals in ms")
	expectedMessages := flag.Uint64("expected-msgs", 0, "Expected messages per flow for exact FCT milestones; 0 disables milestone logging")
	expectedBytes := flag.Uint64("expected-bytes", 0, "Expected payload bytes per flow; 0 disables byte-count validation")
	flag.Parse()

	fmt.Printf("🚀 MP-QUIC Receiver listening on port %s\n", *port)
	if *expectedMessages > 0 {
		fmt.Printf("📐 Exact FCT milestones enabled for %d messages per flow\n", *expectedMessages)
	}

	intervals := parseCSVIntervals(*csvIntervals)
	var tputLogger *throughputCSVLogger
	if *csvPath != "" {
		logger, err := newThroughputCSVLogger(*csvPath)
		if err != nil {
			log.Fatal("Failed to create throughput CSV:", err)
		}
		tputLogger = logger
		defer tputLogger.Close()
		fmt.Printf("📈 Writing throughput samples to %s\n", *csvPath)
	}

	// Load or Generate Certs
	tlsConfig, err := loadTLSConfig(*certFile, *keyFile)
	if err != nil {
		fmt.Printf("⚠️  Could not load certs from disk, generating in-memory...\n")
		tlsConfig, err = generateTLSConfig()
		if err != nil {
			log.Fatal("Failed to generate TLS config:", err)
		}
	} else {
		fmt.Printf("🔐 Loaded TLS certificates from disk\n")
	}

	quicConfig := &quic.Config{
		Versions:         []quic.VersionNumber{mpquicVersion},
		CreatePaths:      *enableMultipath,
		KeepAlive:        true,
		IdleTimeout:      30 * time.Second,
		HandshakeTimeout: 15 * time.Second,
		// MaxReceiveStreamFlowControlWindow:     32 * 1024 * 1024,
		// MaxReceiveConnectionFlowControlWindow: 48 * 1024 * 1024,
	}
	fmt.Printf("🧭 QUIC Version: %d | CreatePaths: %t | Congestion Control: %s\n", mpquicVersion, *enableMultipath, additionalPathCongestionControl)

	listener, err := quic.ListenAddr("0.0.0.0:"+*port, tlsConfig, quicConfig)
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}
	defer listener.Close()

	fmt.Printf("✅ Receiver ready, waiting for connections...\n")

	sessionCount := 0
	for {
		session, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		sessionCount++
		fmt.Printf("✅ New connection #%d from: %s\n", sessionCount, session.RemoteAddr())
		go handleConnection(session, sessionCount, tputLogger, intervals, *expectedMessages, *expectedBytes)
	}
}

type throughputCSVLogger struct {
	mu     sync.Mutex
	file   *os.File
	writer *csv.Writer
	start  time.Time
}

func newThroughputCSVLogger(path string) (*throughputCSVLogger, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"elapsed_ms",
		"interval_ms",
		"session",
		"remote_addr",
		"total_msgs",
		"total_bytes",
		"delta_bytes",
		"throughput_mbps",
	}); err != nil {
		file.Close()
		return nil, err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		file.Close()
		return nil, err
	}
	return &throughputCSVLogger{file: file, writer: writer, start: time.Now()}, nil
}

func (l *throughputCSVLogger) Log(interval time.Duration, sessionNum int, remoteAddr string, totalMsgs, totalBytes, deltaBytes uint64, mbps float64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	elapsedMS := time.Since(l.start).Seconds() * 1000.0
	row := []string{
		strconv.FormatFloat(elapsedMS, 'f', 3, 64),
		strconv.FormatInt(interval.Milliseconds(), 10),
		strconv.Itoa(sessionNum),
		remoteAddr,
		strconv.FormatUint(totalMsgs, 10),
		strconv.FormatUint(totalBytes, 10),
		strconv.FormatUint(deltaBytes, 10),
		strconv.FormatFloat(mbps, 'f', 6, 64),
	}
	if err := l.writer.Write(row); err != nil {
		fmt.Printf("⚠️  CSV write failed: %v\n", err)
		return
	}
	l.writer.Flush()
	if err := l.writer.Error(); err != nil {
		fmt.Printf("⚠️  CSV flush failed: %v\n", err)
	}
}

func (l *throughputCSVLogger) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writer.Flush()
	l.file.Close()
}

func parseCSVIntervals(raw string) []time.Duration {
	var intervals []time.Duration
	for _, part := range strings.Split(raw, ",") {
		ms, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || ms <= 0 {
			continue
		}
		intervals = append(intervals, time.Duration(ms)*time.Millisecond)
	}
	if len(intervals) == 0 {
		return []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}
	}
	return intervals
}

type fctMilestone struct {
	target  uint64
	elapsed time.Duration
	reached bool
}

type flowFCTMilestones struct {
	expectedMessages uint64
	expectedBytes    uint64
	p95              fctMilestone
	p99              fctMilestone
	p100             fctMilestone
	completionTimes  []time.Duration
	finalized        bool
}

func nearestRankTarget(total, percentile uint64) uint64 {
	if total == 0 || percentile == 0 {
		return 0
	}
	if percentile >= 100 {
		return total
	}
	whole := (total / 100) * percentile
	remainder := (total % 100) * percentile
	return whole + (remainder+99)/100
}

func newFlowFCTMilestones(expectedMessages, expectedBytes uint64) flowFCTMilestones {
	return flowFCTMilestones{
		expectedMessages: expectedMessages,
		expectedBytes:    expectedBytes,
		p95:              fctMilestone{target: nearestRankTarget(expectedMessages, 95)},
		p99:              fctMilestone{target: nearestRankTarget(expectedMessages, 99)},
		p100:             fctMilestone{target: expectedMessages},
	}
}

func (m *flowFCTMilestones) observe(elapsed time.Duration) {
	if m.expectedMessages == 0 {
		return
	}
	m.completionTimes = append(m.completionTimes, elapsed)
}

func (m *flowFCTMilestones) finalize() {
	if m.finalized || m.expectedMessages == 0 {
		return
	}
	m.finalized = true
	sort.Slice(m.completionTimes, func(i, j int) bool {
		return m.completionTimes[i] < m.completionTimes[j]
	})
	for _, milestone := range []*fctMilestone{&m.p95, &m.p99, &m.p100} {
		if milestone.target == 0 || uint64(len(m.completionTimes)) < milestone.target {
			continue
		}
		milestone.elapsed = m.completionTimes[milestone.target-1]
		milestone.reached = true
	}
}

func (m flowFCTMilestones) status(receivedMessages, receivedBytes uint64) string {
	switch {
	case m.expectedMessages == 0:
		return "disabled"
	case receivedMessages < m.expectedMessages:
		return "incomplete"
	case receivedMessages > m.expectedMessages:
		return "count_mismatch"
	case m.expectedBytes > 0 && receivedBytes != m.expectedBytes:
		return "byte_mismatch"
	case !m.p95.reached || !m.p99.reached || !m.p100.reached:
		return "incomplete"
	default:
		return "complete"
	}
}

func formatFCTMilestone(milestone fctMilestone) string {
	if !milestone.reached {
		return "NA"
	}
	return strconv.FormatFloat(milestone.elapsed.Seconds(), 'f', 6, 64)
}

func (m *flowFCTMilestones) summaryLine(sessionNum int, remoteAddr string, receivedMessages, receivedBytes uint64) string {
	m.finalize()
	return fmt.Sprintf(
		"[fct_summary] session=%d remote=%s expected_msgs=%d received_msgs=%d "+
			"expected_bytes=%d received_bytes=%d "+
			"fct95_s=%s fct99_s=%s fct100_s=%s method=nearest_rank "+
			"metric=message_completion clock=monotonic status=%s",
		sessionNum,
		remoteAddr,
		m.expectedMessages,
		receivedMessages,
		m.expectedBytes,
		receivedBytes,
		formatFCTMilestone(m.p95),
		formatFCTMilestone(m.p99),
		formatFCTMilestone(m.p100),
		m.status(receivedMessages, receivedBytes),
	)
}

type streamCompletion struct {
	bytes       int
	completedAt time.Time
}

func handleConnection(session quic.Session, sessionNum int, tputLogger *throughputCSVLogger, intervals []time.Duration, expectedMessages, expectedBytes uint64) {
	// Channel to receive byte counts from streams
	statsChan := make(chan streamCompletion, 100)
	// Channel to signal the monitor to stop
	doneChan := make(chan struct{})
	remoteAddr := session.RemoteAddr().String()
	startTime := time.Now()
	var streamWG sync.WaitGroup

	// 1. Start the Monitor Loop (Calculates Throughput)
	go func() {
		var totalBytes uint64
		var totalMsgs uint64

		milestones := newFlowFCTMilestones(expectedMessages, expectedBytes)

		// Variables for interval calculation
		var lastBytes uint64
		lastTime := startTime
		lastCSVBytes := make([]uint64, len(intervals))
		tickerChan := make(chan int, len(intervals)*2)
		tickerDone := make(chan struct{})

		if tputLogger != nil {
			for idx, interval := range intervals {
				ticker := time.NewTicker(interval)
				go func(idx int, ticker *time.Ticker) {
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							select {
							case tickerChan <- idx:
							case <-tickerDone:
								return
							}
						case <-tickerDone:
							return
						}
					}
				}(idx, ticker)
			}
		}

		defer close(tickerDone)

		for {
			select {
			case completion, ok := <-statsChan:
				if !ok {
					// Calculate Flow Completion Time
					endTime := time.Now()
					fct := endTime.Sub(startTime).Seconds()
					timeStr := endTime.Format("15:04:05.000")

					// Final Summary when channel closes
					fmt.Printf("[%s] 🔚 Session #%d closed | Remote: %s | Total: %d msgs | %d bytes (%.2f MB) | FCT: %.4f s\n",
						timeStr, sessionNum, remoteAddr, totalMsgs, totalBytes, float64(totalBytes)/1024/1024, fct)
					if expectedMessages > 0 {
						fmt.Printf("[%s] %s\n", timeStr, milestones.summaryLine(sessionNum, remoteAddr, totalMsgs, totalBytes))
					}

					close(doneChan) // Signal that logging is finished
					return
				}

				totalBytes += uint64(completion.bytes)
				totalMsgs++
				milestones.observe(completion.completedAt.Sub(startTime))

				// Log on 1st message or every 20 message
				if totalMsgs == 1 || totalMsgs%20 == 0 {
					now := time.Now()
					timeStr := now.Format("15:04:05.000") // Format: HH:MM:SS.ms

					// Calculate interval stats
					deltaBytes := totalBytes - lastBytes
					deltaTime := now.Sub(lastTime).Seconds()

					// Avoid divide by zero if it's super fast
					if deltaTime == 0 {
						deltaTime = 0.0001
					}

					// Mbps calculation: (Bytes * 8) / (Seconds * 1,000,000)
					mbps := (float64(deltaBytes) * 8.0) / (deltaTime * 1000000.0)

					if totalMsgs == 1 {
						fmt.Printf("[%s] 🔹 Session #%d | First Packet Received (%d bytes)\n", timeStr, sessionNum, completion.bytes)
					} else {
						fmt.Printf("[%s] 📊 Session #%d | Msgs: %d | Last 2 Avg: %.2f Mbps\n",
							timeStr, sessionNum, totalMsgs, mbps)
					}

					// Reset interval trackers
					lastBytes = totalBytes
					lastTime = now
				}

			case idx := <-tickerChan:
				if idx < 0 || idx >= len(intervals) {
					continue
				}
				interval := intervals[idx]
				deltaBytes := totalBytes - lastCSVBytes[idx]
				lastCSVBytes[idx] = totalBytes
				mbps := (float64(deltaBytes) * 8.0) / (interval.Seconds() * 1000000.0)
				tputLogger.Log(interval, sessionNum, remoteAddr, totalMsgs, totalBytes, deltaBytes, mbps)
			}
		}
	}()

	defer func() {
		streamWG.Wait()  // Ensure every completed stream contributes its timestamp.
		close(statsChan) // Stop the monitor loop
		<-doneChan       // Wait for monitor to print final summary
		session.Close(nil)
	}()

	// 2. Accept Streams
	for {
		stream, err := session.AcceptStream()
		if err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "PeerGoingAway") {
				fmt.Printf("⚠️  Session #%d error: %v\n", sessionNum, err)
			}
			return
		}
		streamWG.Add(1)
		go func(acceptedStream quic.Stream) {
			defer streamWG.Done()
			handleStream(acceptedStream, statsChan)
		}(stream)
	}
}

func handleStream(stream quic.Stream, statsChan chan<- streamCompletion) {
	defer stream.Close()

	data, err := io.ReadAll(stream)
	if err != nil {
		return
	}

	// Send the byte count to the monitor loop
	statsChan <- streamCompletion{bytes: len(data), completedAt: time.Now()}

	// Send ACK
	ackMsg := fmt.Sprintf("ACK: Received %d bytes", len(data))
	stream.Write([]byte(ackMsg))
}

// --- TLS Helpers (Same as before) ---

func loadTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"quic-data"},
	}, nil
}

func generateTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"MP-QUIC Test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("0.0.0.0"),
		},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: key}
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"quic-data"}}, nil
}
