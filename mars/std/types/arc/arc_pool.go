package arc

import (
	"github.com/named-data/ndnd/std/types/sync_pool"
)

// ArcPool is a sync pool based on reference counting.
// When the counter reaches zero, the value is returned to the pool.
type ArcPool[T any] struct {
	sync_pool.SyncPool[*Arc[T]]
}

// NewArcPool creates a new ArcPool[T].
func NewArcPool[T any](init func() T, reset func(T)) *ArcPool[T] {
	pool := &ArcPool[T]{}
	pool.SyncPool = sync_pool.New(
		func() *Arc[T] { return &Arc[T]{v: init(), p: pool} },
		func(val *Arc[T]) { reset(val.Load()) })
	return pool
}
