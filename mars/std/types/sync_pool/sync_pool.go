// sync_pool is a generic sync.Pool wrapper
package sync_pool

import "sync"

type SyncPool[T any] struct {
	pool  sync.Pool
	reset func(T)
}

// New creates a new Pool[T].
func New[T any](init func() T, reset func(T)) SyncPool[T] {
	return SyncPool[T]{
		pool: sync.Pool{
			New: func() any { return init() },
		},
		reset: reset,
	}
}

// New creates a new object of type T.
func (p *SyncPool[T]) New() T {
	val := p.pool.New().(T)
	p.reset(val)
	return val
}

// Get returns a new T from the pool.
func (p *SyncPool[T]) Get() T {
	val := p.pool.Get().(T)
	p.reset(val)
	return val
}

// Put returns a T to the pool.
func (p *SyncPool[T]) Put(val T) {
	p.pool.Put(val)
}
