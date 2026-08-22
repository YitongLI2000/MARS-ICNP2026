/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2021 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package fw

import (
	"sort"
	"strconv"
	"strings"

	"github.com/named-data/ndnd/fw/core"
	"github.com/named-data/ndnd/fw/defn"
	"github.com/named-data/ndnd/fw/table"
	enc "github.com/named-data/ndnd/std/encoding"
)

// BestRouteInterestForwardingMode selects how BestRoute chooses nexthops.
type BestRouteInterestForwardingMode string

const (
	// BestRouteInterestForwardingStandard keeps the current two-pass fallback behavior.
	BestRouteInterestForwardingStandard BestRouteInterestForwardingMode = "standard"
	// BestRouteInterestForwardingStrictLeastCost always uses only the least-cost nexthop.
	BestRouteInterestForwardingStrictLeastCost BestRouteInterestForwardingMode = "strict-least-cost"
)

// BestRouteInterestForwardingType switches forwarding behavior for BestRoute.
// Change this constant to BestRouteInterestForwardingStrictLeastCost to force
// strict least-cost forwarding.
// const BestRouteInterestForwardingType = BestRouteInterestForwardingStandard
const BestRouteInterestForwardingType = BestRouteInterestForwardingStrictLeastCost

// BestRoute is a forwarding strategy that forwards Interests
// to the nexthop with the lowest cost.
type BestRoute struct {
	StrategyBase
	forwardingMode BestRouteInterestForwardingMode
}

func init() {
	strategyInit = append(strategyInit, func() Strategy { return &BestRoute{} })
	StrategyVersions["best-route"] = []uint64{1}
}

func (s *BestRoute) Instantiate(fwThread *Thread) {
	s.NewStrategyBase(fwThread, "best-route", 1)
	s.forwardingMode = BestRouteInterestForwardingType
	if s.forwardingMode != BestRouteInterestForwardingStandard &&
		s.forwardingMode != BestRouteInterestForwardingStrictLeastCost {
		core.Log.Warn(s, "Unknown BestRoute forwarding mode, fallback to standard",
			"mode", s.forwardingMode)
		s.forwardingMode = BestRouteInterestForwardingStandard
	}
}

func (s *BestRoute) AfterContentStoreHit(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
) {
	core.Log.Trace(s, "AfterContentStoreHit", "name", packet.Name, "faceid", inFace)
	s.SendData(packet, pitEntry, inFace, 0) // 0 indicates ContentStore is source
}

func (s *BestRoute) AfterReceiveData(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
) {
	core.Log.Trace(s, "AfterReceiveData", "name", packet.Name, "inrecords", len(pitEntry.InRecords()))

	// Extract sequence number from data name and log every 200 packets
	dataName := packet.Name.String()
	seqNum := extractSeqNum(dataName)
	if seqNum >= 0 && seqNum%200 == 0 {
		prefixKey := getPrefixKey(packet.Name)
		core.Log.Debug(s, "Data packet logged", "name", packet.Name, "prefix", prefixKey, "seq", seqNum)
	}

	for faceID := range pitEntry.InRecords() {
		core.Log.Trace(s, "Forwarding Data", "name", packet.Name, "faceid", faceID)
		s.SendData(packet, pitEntry, faceID, inFace)
	}
}

// AfterReceiveInterest forwards an Interest according to the configured BestRoute mode.
func (s *BestRoute) AfterReceiveInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	// Extract sequence number from interest name and log every 200 packets
	interestName := packet.Name.String()
	seqNum := extractSeqNum(interestName)
	if seqNum >= 0 && seqNum%200 == 0 {
		prefixKey := getPrefixKey(packet.Name)
		core.Log.Debug(s, "Interest packet logged", "name", packet.Name, "prefix", prefixKey, "seq", seqNum)
	}

	if len(nexthops) == 0 {
		// core.Log.Debug(s, "No nexthop found - DROP", "name", packet.Name)
		core.Log.Trace(s, "No nexthop found - DROP", "name", packet.Name)
		return
	}

	sortNexthopsByCost(nexthops)

	switch s.forwardingMode {
	case BestRouteInterestForwardingStrictLeastCost:
		s.forwardInterestStrictLeastCost(packet, pitEntry, inFace, nexthops)
	default:
		s.forwardInterestStandard(packet, pitEntry, inFace, nexthops)
	}
}

// BeforeSatisfyInterest is a no-op for BestRoute.
func (s *BestRoute) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {
}

// AfterReceiveNack forwards a NACK to each downstream face except its ingress.
func (s *BestRoute) AfterReceiveNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
	core.Log.Trace(s, "AfterReceiveNack", "name", packet.Name, "reason", nackReason, "inFace", inFace)

	// Forward NACK to all downstream faces (in-records) except the incoming face
	for faceID := range pitEntry.InRecords() {
		if faceID != inFace {
			s.SendNack(packet, pitEntry, faceID, inFace, nackReason)
		}
	}
}

// AfterReceiveLoopedInterest drops a looped Interest.
func (s *BestRoute) AfterReceiveLoopedInterest(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	core.Log.Trace(s, "AfterReceiveLoopedInterest", "name", packet.Name, "inFace", inFace)
}

func sortNexthopsByCost(nexthops []*table.FibNextHopEntry) {
	sort.Slice(nexthops, func(i, j int) bool {
		if nexthops[i].Cost == nexthops[j].Cost {
			return nexthops[i].Nexthop < nexthops[j].Nexthop
		}
		return nexthops[i].Cost < nexthops[j].Cost
	})
}

func (s *BestRoute) forwardInterestStandard(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	for pass := range 2 {
		for _, nh := range nexthops {
			// In the first pass, skip hops that already have an out-record.
			if pass == 0 {
				if oR := pitEntry.OutRecords()[nh.Nexthop]; oR != nil {
					continue
				}
			}

			// For the second pass, we should ideally use the least recently tried hop.
			// But then we need to resort the list - this is just faster for now.
			// In densely connected networks, this is not a big deal.
			core.Log.Trace(s, "Forwarding Interest", "name", packet.Name, "faceid", nh.Nexthop)
			if sent := s.SendInterest(packet, pitEntry, nh.Nexthop, inFace); sent {
				return
			}
		}
	}

	// core.Log.Debug(s, "No usable nexthop for Interest - DROP", "name", packet.Name)
	core.Log.Trace(s, "No usable nexthop for Interest - DROP", "name", packet.Name)
}

func (s *BestRoute) forwardInterestStrictLeastCost(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	leastCost := nexthops[0]
	core.Log.Trace(s, "Forwarding Interest (strict least-cost)",
		"name", packet.Name, "faceid", leastCost.Nexthop, "cost", leastCost.Cost)
	if sent := s.SendInterest(packet, pitEntry, leastCost.Nexthop, inFace); sent {
		return
	}

	core.Log.Trace(s, "Least-cost nexthop unusable - DROP",
		"name", packet.Name, "faceid", leastCost.Nexthop, "cost", leastCost.Cost)
}

// getPrefixKey extracts the prefix key from a packet name.
// Uses the first component as the prefix key.
func getPrefixKey(name enc.Name) string {
	if len(name) > 0 {
		return name[0].String()
	}
	return "/"
}

// extractSeqNum extracts the sequence number from a name string.
// Expected format: /prefix/node/[pd/<index>/|dt/]seq-<num>
// Returns -1 if parsing fails.
func extractSeqNum(nameStr string) int {
	parts := strings.Split(nameStr, "/")
	if len(parts) == 0 {
		return -1
	}
	// Last component should be "seq-N"
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
