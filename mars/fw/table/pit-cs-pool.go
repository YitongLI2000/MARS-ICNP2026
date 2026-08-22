package table

import (
	"github.com/named-data/ndnd/std/types/sync_pool"
)

type PitCsPoolsT struct {
	PitInRecord      sync_pool.SyncPool[*PitInRecord]
	PitOutRecord     sync_pool.SyncPool[*PitOutRecord]
	NameTreePitEntry sync_pool.SyncPool[*nameTreePitEntry]
	PitCsTreeNode    sync_pool.SyncPool[*pitCsTreeNode]
}

var PitCsPools = &PitCsPoolsT{
	PitInRecord: sync_pool.New(
		func() *PitInRecord { return &PitInRecord{} },
		func(obj *PitInRecord) {
			// Do not reuse the PitToken array since it is passed
			// to the outgoing pipeline without copying.
			obj.PitToken = make([]byte, 0, 8)
		},
	),

	PitOutRecord: sync_pool.New(
		func() *PitOutRecord { return &PitOutRecord{} },
		func(obj *PitOutRecord) {},
	),

	NameTreePitEntry: sync_pool.New(
		func() *nameTreePitEntry {
			entry := &nameTreePitEntry{}
			entry.inRecords = make(map[uint64]*PitInRecord)
			entry.outRecords = make(map[uint64]*PitOutRecord)
			return entry
		},
		func(obj *nameTreePitEntry) {
			clear(obj.inRecords)
			clear(obj.outRecords)
		},
	),

	PitCsTreeNode: sync_pool.New(
		func() *pitCsTreeNode {
			return &pitCsTreeNode{
				children:   make(map[uint64]*pitCsTreeNode),
				pitEntries: make([]*nameTreePitEntry, 0, 4),
			}
		},
		func(obj *pitCsTreeNode) {
			clear(obj.children)
			obj.name = nil
			obj.pitEntries = obj.pitEntries[:0]
			obj.csEntry = nil
		},
	),
}
