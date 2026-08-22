package main

import (
	"crypto/rand"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/lucas-clemente/quic-go"
)

const mpquicVersion quic.VersionNumber = 512
const additionalPathCongestionControl = "olia"

func main() {
	addr := flag.String("addr", "localhost:6121", "Server address")
	count := flag.Int("count", 1000, "Number of messages to send")
	senderName := flag.String("name", "Sender", "Name of this sender")
	autoPath := flag.Bool("auto", true, "Auto-detect multipath capability")
	forceSingle := flag.Bool("single", false, "Force single path mode")
	forceMultipath := flag.Bool("force-multipath", false, "Force multipath mode even if route probing does not detect ECMP")
	bindIP := flag.String("bind", "", "Local IP to bind to (optional)")
	msgSize := flag.Int("size", 1024, "Size of each message in bytes")
	totalTarget := flag.Int("total", 0, "Exact total payload bytes to send; overrides count when set")
	pipeline := flag.Int("pipeline", 32, "Maximum number of chunks in flight")
	rateMbps := flag.Float64("rate-mbps", 0, "Optional per-flow application payload pacing rate in Mbps")
	flag.Parse()

	if *msgSize <= 0 {
		log.Fatal("message size must be positive")
	}
	if *totalTarget < 0 {
		log.Fatal("total payload bytes cannot be negative")
	}
	if *pipeline <= 0 {
		log.Fatal("pipeline must be positive")
	}
	if *rateMbps < 0 {
		log.Fatal("rate-mbps cannot be negative")
	}

	messageCount := *count
	totalBytesTarget := uint64(*count) * uint64(*msgSize)
	finalChunkSize := *msgSize
	if *totalTarget > 0 {
		messageCount = *totalTarget / *msgSize
		finalChunkSize = *msgSize
		if remainder := *totalTarget % *msgSize; remainder != 0 {
			messageCount++
			finalChunkSize = remainder
		}
		totalBytesTarget = uint64(*totalTarget)
	}

	fmt.Printf("🚀 MP-QUIC Sender [%s] connecting to %s\n", *senderName, *addr)
	fmt.Printf("📦 Target Packet Size: %d bytes | Count: %d\n", *msgSize, messageCount)
	fmt.Printf("🧵 Pipeline Window: %d chunks\n", *pipeline)
	if *forceMultipath {
		fmt.Printf("🛣️  Multipath mode forced on\n")
	}
	if *rateMbps > 0 {
		fmt.Printf("⏳ Per-flow pacing: %.3f Mbps\n", *rateMbps)
	}
	if *totalTarget > 0 {
		fmt.Printf("🎯 Exact Total Payload: %d bytes | Final Chunk: %d bytes\n", *totalTarget, finalChunkSize)
	}

	// 1. Determine Destination and Paths
	paths, err := discoverPaths(*addr)
	if err != nil {
		fmt.Printf("⚠️  Path discovery failed: %v, using single path\n", err)
		paths = 1
	}

	useMultipath := !*forceSingle && (*forceMultipath || (*autoPath && paths > 1))

	// 2. Setup TLS and QUIC Config
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"quic-data"},
		ServerName:         "localhost",
	}

	quicConfig := &quic.Config{
		Versions:         []quic.VersionNumber{mpquicVersion},
		CreatePaths:      useMultipath,
		KeepAlive:        true,
		IdleTimeout:      30 * time.Second,
		HandshakeTimeout: 15 * time.Second,
		// MaxReceiveStreamFlowControlWindow:     2 * 1024 * 1024,
		// MaxReceiveConnectionFlowControlWindow: 3 * 1024 * 1024,
	}
	fmt.Printf("🧭 QUIC Version: %d | CreatePaths: %t | Congestion Control: %s\n", mpquicVersion, useMultipath, additionalPathCongestionControl)

	// 3. Bind to the correct local IP
	localIP := *bindIP
	if localIP == "" {
		localIP = findDataNetworkIP()
	}

	var udpConn *net.UDPConn
	if localIP != "" {
		fmt.Printf("📌 Binding to local data IP: %s\n", localIP)
		localAddr, err := net.ResolveUDPAddr("udp", localIP+":0")
		if err != nil {
			log.Fatal("Failed to resolve local address:", err)
		}
		udpConn, err = net.ListenUDP("udp", localAddr)
		if err != nil {
			log.Fatal("Failed to bind UDP socket:", err)
		}
	} else {
		fmt.Println("⚠️  Warning: Could not find 10.x IP, binding to 0.0.0.0")
		localAddr, _ := net.ResolveUDPAddr("udp", ":0")
		udpConn, _ = net.ListenUDP("udp", localAddr)
	}

	// 4. Resolve Remote Address
	udpAddr, err := net.ResolveUDPAddr("udp", *addr)
	if err != nil {
		log.Fatal("Failed to resolve remote address:", err)
	}

	// 5. Dial
	session, err := quic.Dial(udpConn, udpAddr, udpAddr.String(), tlsConfig, quicConfig, nil)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer session.Close(nil)

	fmt.Printf("✅ Connected to: %s\n", session.RemoteAddr())

	// --- PREPARE DATA ---
	// Generate the payload once to avoid CPU overhead during the loop
	payload := make([]byte, *msgSize)
	rand.Read(payload) // Fill with random data

	fmt.Println("🔥 Starting transmission ASAP...")
	startTime := time.Now()

	// 6. Send Messages Loop
	type sendResult struct {
		bytes int
		err   error
	}

	var totalBytesSent uint64
	var nextSendTime time.Time
	var rateBytesPerSecond float64
	if *rateMbps > 0 {
		nextSendTime = time.Now()
		rateBytesPerSecond = (*rateMbps * 1000000.0) / 8.0
	}

	sem := make(chan struct{}, *pipeline)
	results := make(chan sendResult, messageCount)
	var wg sync.WaitGroup

	for i := 0; i < messageCount; i++ {
		chunk := payload
		remaining := totalBytesTarget - totalBytesSent
		if remaining < uint64(len(chunk)) {
			chunk = payload[:int(remaining)]
		}

		if *rateMbps > 0 {
			if sleepFor := time.Until(nextSendTime); sleepFor > 0 {
				time.Sleep(sleepFor)
			}
			chunkDuration := time.Duration((float64(len(chunk)) / rateBytesPerSecond) * float64(time.Second))
			nextSendTime = nextSendTime.Add(chunkDuration)
		}

		totalBytesSent += uint64(len(chunk))
		sem <- struct{}{}
		wg.Add(1)
		go func(msgNum int, data []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			err := sendMessage(session, msgNum, useMultipath, *senderName, data)
			if err != nil {
				results <- sendResult{err: fmt.Errorf("message %d: %w", msgNum, err)}
				return
			}
			results <- sendResult{bytes: len(data)}
		}(i+1, chunk)

		// Progress bar every 10%
		if messageCount >= 10 && (i+1)%(messageCount/10) == 0 {
			fmt.Printf(".")
		}
	}
	wg.Wait()
	close(results)
	fmt.Println() // Newline after progress dots

	// --- STATISTICS ---
	duration := time.Since(startTime)
	var totalBytesAcked uint64
	var errorCount int
	for result := range results {
		if result.err != nil {
			errorCount++
			fmt.Printf("❌ Error sending %v\n", result.err)
			continue
		}
		totalBytesAcked += uint64(result.bytes)
	}

	bits := float64(totalBytesAcked) * 8.0
	mbps := (bits / duration.Seconds()) / 1000000.0

	fmt.Printf("🎉 All messages sent!\n")
	fmt.Printf("⏱️  Time: %v\n", duration)
	fmt.Printf("📊 Total Data: %d bytes (%.2f MB)\n", totalBytesAcked, float64(totalBytesAcked)/1024/1024)
	if errorCount > 0 {
		fmt.Printf("⚠️  Failed Messages: %d\n", errorCount)
	}
	fmt.Printf("🚀 Throughput: %.2f Mbps\n", mbps)
}

// findDataNetworkIP looks for a local interface IP starting with "10."
func findDataNetworkIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipStr := ipnet.IP.String()
				if strings.HasPrefix(ipStr, "10.") {
					return ipStr
				}
			}
		}
	}
	return ""
}

func discoverPaths(addr string) (int, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return 1, err
	}
	cmd := exec.Command("ip", "route", "get", host)
	output, err := cmd.Output()
	if err == nil {
		outputStr := string(output)
		if strings.Count(outputStr, "nexthop") > 0 || strings.Contains(outputStr, "multipath") {
			return 2, nil
		}
	}
	return 1, nil
}

func sendMessage(session quic.Session, msgNum int, multipath bool, senderName string, payload []byte) error {
	// Each chunk uses one bidirectional QUIC stream.
	stream, err := session.OpenStreamSync()
	if err != nil {
		return fmt.Errorf("failed to open stream: %v", err)
	}
	defer stream.Close()

	// Write the actual payload
	_, err = stream.Write(payload)
	if err != nil {
		return fmt.Errorf("failed to write: %v", err)
	}

	// Close the write-side of the stream to signal EOF to receiver
	err = stream.Close()
	if err != nil {
		return fmt.Errorf("failed to close write: %v", err)
	}

	// Wait for the receiver's application-level acknowledgment before completing this chunk.
	_, err = io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("failed to read ACK: %v", err)
	}

	return nil
}
