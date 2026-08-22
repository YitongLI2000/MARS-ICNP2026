/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2021 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package table

import (
	"time"

	"github.com/named-data/ndnd/fw/defn"
	enc "github.com/named-data/ndnd/std/encoding"
)

// PitCsTable dictates what functionality a Pit-Cs table should implement
// Warning: All functions must be called in the same forwarding goroutine as the creation of the table.
type PitCsTable interface {
	// InsertInterest inserts an Interest into the PIT.
	InsertInterest(interest *defn.FwInterest, hint enc.Name, inFace uint64) (PitEntry, bool)
	// RemoveInterest removes an Interest from the PIT.
	RemoveInterest(pitEntry PitEntry) bool
	// FindInterestExactMatch finds an exact match for an Interest in the PIT.
	FindInterestExactMatchEnc(interest *defn.FwInterest) PitEntry
	// FindInterestPrefixMatchByDataEnc finds a prefix match for a Data in the PIT.
	FindInterestPrefixMatchByDataEnc(data *defn.FwData, token *uint32) []PitEntry
	// PitSize returns the number of entries in the PIT.
	PitSize() int

	// InsertData inserts a Data into the CS.
	InsertData(data *defn.FwData, wire []byte)
	// FindMatchingDataFromCS finds a matching Data in the CS.
	FindMatchingDataFromCS(interest *defn.FwInterest) CsEntry
	// CsSize returns the number of entries in the CS.
	CsSize() int
	// IsCsAdmitting returns whether the CS is admitting new entries.
	IsCsAdmitting() bool
	// IsCsServing returns whether the CS is serving entries.
	IsCsServing() bool

	// UpdateTicker returns the channel used to signal regular Update() calls in the forwarding thread.
	UpdateTicker() <-chan time.Time
	// Update() does whatever the PIT table needs to do regularly.
	Update()

	// eraseCsDataFromReplacementStrategy removes a Data from the replacement strategy.
	eraseCsDataFromReplacementStrategy(index uint64)
	// updatePitExpiry updates the PIT entry's expiration time.
	updatePitExpiry(pitEntry PitEntry)
}

// PitEntry dictates what entries in a PIT-CS table should implement
type PitEntry interface {
	PitCs() PitCsTable
	EncName() enc.Name
	CanBePrefix() bool
	MustBeFresh() bool

	// Interests must match in terms of Forwarding Hint to be aggregated in PIT.
	ForwardingHintNew() enc.Name

	InRecords() map[uint64]*PitInRecord   // Key is face ID
	OutRecords() map[uint64]*PitOutRecord // Key is face ID

	ExpirationTime() time.Time
	setExpirationTime(t time.Time) // use table.UpdateExpirationTimer()

	Satisfied() bool
	SetSatisfied(isSatisfied bool)

	Token() uint32

	InsertInRecord(interest *defn.FwInterest, face uint64, incomingPitToken []byte) (*PitInRecord, bool, uint32)
	InsertOutRecord(interest *defn.FwInterest, face uint64) *PitOutRecord

	RemoveInRecord(face uint64)
	RemoveOutRecord(face uint64)
	ClearOutRecords()
	ClearInRecords()
}

// basePitEntry contains PIT entry properties common to all tables.
type basePitEntry struct {
	// lowercase fields so that they aren't exported
	encname           enc.Name
	canBePrefix       bool
	mustBeFresh       bool
	forwardingHintNew enc.Name
	// Interests must match in terms of Forwarding Hint to be
	// aggregated in PIT.
	inRecords      map[uint64]*PitInRecord  // Key is face ID
	outRecords     map[uint64]*PitOutRecord // Key is face ID
	expirationTime time.Time
	satisfied      bool

	token uint32
}

// PitInRecord records an incoming Interest on a given face.
type PitInRecord struct {
	Face            uint64
	LatestTimestamp time.Time
	LatestNonce     uint32
	ExpirationTime  time.Time
	PitToken        []byte
}

// PitOutRecord records an outgoing Interest on a given face.
type PitOutRecord struct {
	Face            uint64
	LatestTimestamp time.Time
	LatestNonce     uint32
	ExpirationTime  time.Time
}

// CsEntry is an entry in a thread's CS.
type CsEntry interface {
	Index() uint64 // the hash of the entry, for fast lookup
	StaleTime() time.Time
	Copy() (*defn.FwData, []byte, error)
}

type baseCsEntry struct {
	index     uint64
	staleTime time.Time
	wire      []byte
}

// InsertInRecord finds or inserts an InRecord for the face, updating the
// metadata and returning whether there was already an in-record in the entry.
// The third return value is the previous nonce if the in-record already existed.
func (bpe *basePitEntry) InsertInRecord(
	interest *defn.FwInterest,
	face uint64,
	incomingPitToken []byte,
) (*PitInRecord, bool, uint32) {
	lifetime := interest.Lifetime().GetOr(time.Millisecond * 4000)

	var record *PitInRecord
	var ok bool
	if record, ok = bpe.inRecords[face]; !ok {
		record := PitCsPools.PitInRecord.Get()
		record.Face = face
		record.LatestNonce = interest.NonceV.Unwrap()
		record.LatestTimestamp = time.Now()
		record.ExpirationTime = time.Now().Add(lifetime)
		record.PitToken = append(record.PitToken, incomingPitToken...)
		bpe.inRecords[face] = record
		return record, false, 0
	}

	// Existing record
	previousNonce := record.LatestNonce
	record.LatestNonce = interest.NonceV.Unwrap()
	record.LatestTimestamp = time.Now()
	record.ExpirationTime = time.Now().Add(lifetime)
	return record, true, previousNonce
}

// InsertOutRecord inserts an outrecord for the given interest, updating the
// preexisting one if it already occcurs.
func (bpe *basePitEntry) InsertOutRecord(interest *defn.FwInterest, face uint64) *PitOutRecord {
	lifetime := interest.Lifetime().GetOr(time.Millisecond * 4000)
	var record *PitOutRecord
	var ok bool
	if record, ok = bpe.outRecords[face]; !ok {
		record := PitCsPools.PitOutRecord.Get()
		record.Face = face
		record.LatestNonce = interest.NonceV.Unwrap()
		record.LatestTimestamp = time.Now()
		record.ExpirationTime = time.Now().Add(lifetime)
		bpe.outRecords[face] = record
		return record
	}

	// Existing record
	record.LatestNonce = interest.NonceV.Unwrap()
	record.LatestTimestamp = time.Now()
	record.ExpirationTime = time.Now().Add(lifetime)
	return record
}

// UpdateExpirationTimer sets the expiration time of the PIT entry.
func UpdateExpirationTimer(e PitEntry, t time.Time) {
	e.setExpirationTime(t)
	e.PitCs().updatePitExpiry(e)
}

// /// Setters and Getters /////
func (bpe *basePitEntry) EncName() enc.Name {
	return bpe.encname
}

func (bpe *basePitEntry) CanBePrefix() bool {
	return bpe.canBePrefix
}

func (bpe *basePitEntry) MustBeFresh() bool {
	return bpe.mustBeFresh
}

func (bpe *basePitEntry) ForwardingHintNew() enc.Name {
	return bpe.forwardingHintNew
}

func (bpe *basePitEntry) InRecords() map[uint64]*PitInRecord {
	return bpe.inRecords
}

func (bpe *basePitEntry) OutRecords() map[uint64]*PitOutRecord {
	return bpe.outRecords
}

func (bpe *basePitEntry) RemoveInRecord(face uint64) {
	if record, ok := bpe.inRecords[face]; ok {
		PitCsPools.PitInRecord.Put(record)
		delete(bpe.inRecords, face)
	}
}

func (bpe *basePitEntry) RemoveOutRecord(face uint64) {
	if record, ok := bpe.outRecords[face]; ok {
		PitCsPools.PitOutRecord.Put(record)
		delete(bpe.outRecords, face)
	}
}

// ClearInRecords removes all in-records from the PIT entry.
func (bpe *basePitEntry) ClearInRecords() {
	for _, record := range bpe.inRecords {
		PitCsPools.PitInRecord.Put(record)
	}
	clear(bpe.inRecords)
}

// ClearOutRecords removes all out-records from the PIT entry.
func (bpe *basePitEntry) ClearOutRecords() {
	for _, record := range bpe.outRecords {
		PitCsPools.PitOutRecord.Put(record)
	}
	clear(bpe.outRecords)
}

func (bpe *basePitEntry) ExpirationTime() time.Time {
	return bpe.expirationTime
}

func (bpe *basePitEntry) setExpirationTime(t time.Time) {
	bpe.expirationTime = t
}

func (bpe *basePitEntry) Satisfied() bool {
	return bpe.satisfied
}

func (bpe *basePitEntry) SetSatisfied(isSatisfied bool) {
	bpe.satisfied = isSatisfied
}

func (bpe *basePitEntry) Token() uint32 {
	return bpe.token
}

func (bce *baseCsEntry) Index() uint64 {
	return bce.index
}

func (bce *baseCsEntry) StaleTime() time.Time {
	return bce.staleTime
}

func (bce *baseCsEntry) Copy() (*defn.FwData, []byte, error) {
	wire := make([]byte, len(bce.wire))
	copy(wire, bce.wire)

	data, err := defn.ParseFwPacket(enc.NewBufferView(wire), false)
	if err != nil {
		return nil, nil, err
	}

	return data.Data, wire, nil
}
