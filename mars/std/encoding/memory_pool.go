package encoding

import (
	"bytes"
	"hash"

	"github.com/cespare/xxhash"
	"github.com/named-data/ndnd/std/types/sync_pool"
)

type hashPoolVal struct {
	hash   hash.Hash64
	buffer bytes.Buffer
}

var xxHashPool = sync_pool.New(
	func() *hashPoolVal {
		return &hashPoolVal{
			hash:   xxhash.New(),
			buffer: bytes.Buffer{},
		}
	},
	func(obj *hashPoolVal) {
		obj.hash.Reset()
		obj.buffer.Reset()
	},
)
