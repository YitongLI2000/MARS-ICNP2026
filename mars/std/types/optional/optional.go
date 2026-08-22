package optional

import "golang.org/x/exp/constraints"

// Optional is a type that represents an optional value
type Optional[T any] struct {
	value T
	isSet bool
}

// IsSet returns true if the optional value is set
func (o Optional[T]) IsSet() bool {
	return o.isSet
}

// Set sets the optional value
func (o *Optional[T]) Set(v T) {
	o.value = v
	o.isSet = true
}

// Unset unsets the optional value
func (o *Optional[T]) Unset() {
	o.isSet = false
}

// Get returns the optional value and a boolean indicating if the value is set
func (o Optional[T]) Get() (T, bool) {
	return o.value, o.isSet
}

// GetOr returns the optional value or a default value if the value is not set
func (o Optional[T]) GetOr(def T) T {
	if o.isSet {
		return o.value
	}
	return def
}

// Unwrap returns the optional value or panics if the value is not set
func (o Optional[T]) Unwrap() T {
	if o.isSet {
		return o.value
	}
	panic("Optional value is not set")
}

// Some creates an optional value with the given value
func Some[T any](v T) Optional[T] {
	return Optional[T]{value: v, isSet: true}
}

// None creates an optional value with no value set
func None[T any]() Optional[T] {
	return Optional[T]{isSet: false}
}

// CastInt converts an integer optional value to another type
func CastInt[A, B constraints.Integer](a Optional[A]) (out Optional[B]) {
	if a.IsSet() {
		out.Set(B(a.Unwrap()))
	}
	return out
}
