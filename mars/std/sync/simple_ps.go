package sync

import (
	"iter"

	enc "github.com/named-data/ndnd/std/encoding"
)

// SimplePs is a simple Pub/Sub system.
type SimplePs[V any] struct {
	// subs is the list of subscribers.
	subs map[string]SimplePsSub[V]
}

type SimplePsSub[V any] struct {
	// Prefix is the name prefix to subscribe.
	Prefix enc.Name
	// Callback is the callback function.
	Callback func(V)
}

func NewSimplePs[V any]() SimplePs[V] {
	return SimplePs[V]{
		subs: make(map[string]SimplePsSub[V]),
	}
}

func (ps *SimplePs[V]) Subscribe(prefix enc.Name, callback func(V)) error {
	if callback == nil {
		panic("Callback is required for subscription")
	}

	ps.subs[prefix.TlvStr()] = SimplePsSub[V]{
		Prefix:   prefix,
		Callback: callback,
	}

	return nil
}

func (ps *SimplePs[V]) Unsubscribe(prefix enc.Name) {
	delete(ps.subs, prefix.TlvStr())
}

func (ps *SimplePs[V]) Subs(prefix enc.Name) iter.Seq[func(V)] {
	return func(yield func(func(V)) bool) {
		for _, sub := range ps.subs {
			if sub.Prefix.IsPrefix(prefix) {
				if !yield(sub.Callback) {
					return
				}
			}
		}
	}
}

func (ps *SimplePs[V]) HasSub(prefix enc.Name) bool {
	for range ps.Subs(prefix) {
		return true
	}
	return false
}

func (ps *SimplePs[V]) Publish(name enc.Name, data V) {
	for sub := range ps.Subs(name) {
		sub(data)
	}
}
