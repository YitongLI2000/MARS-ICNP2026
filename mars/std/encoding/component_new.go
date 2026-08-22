package encoding

import "time"

// VersionImmutable is the version number for immutable objects.
// A version number of 0 will be used on the wire.
const VersionImmutable = uint64(0)

// VersionUnixMicro is the version number for objects with a unix timestamp.
// A version number of microseconds since the unix epoch will be used on the wire.
// Current unix time must be positive, or usage will panic.
const VersionUnixMicro = uint64(1<<63 - 16)

func NewBytesComponent(typ TLNum, val []byte) Component {
	return Component{
		Typ: typ,
		Val: val,
	}
}

func NewStringComponent(typ TLNum, val string) Component {
	return Component{
		Typ: typ,
		Val: []byte(val),
	}
}

func NewNumberComponent(typ TLNum, val uint64) Component {
	return Component{
		Typ: typ,
		Val: Nat(val).Bytes(),
	}
}

func NewGenericComponent(val string) Component {
	return NewStringComponent(TypeGenericNameComponent, val)
}

func NewGenericBytesComponent(val []byte) Component {
	return NewBytesComponent(TypeGenericNameComponent, val)
}

func NewKeywordComponent(val string) Component {
	return NewStringComponent(TypeKeywordNameComponent, val)
}

func NewSegmentComponent(seg uint64) Component {
	return NewNumberComponent(TypeSegmentNameComponent, seg)
}

func NewByteOffsetComponent(off uint64) Component {
	return NewNumberComponent(TypeByteOffsetNameComponent, off)
}

func NewSequenceNumComponent(seq uint64) Component {
	return NewNumberComponent(TypeSequenceNumNameComponent, seq)
}

func NewVersionComponent(v uint64) Component {
	return NewNumberComponent(TypeVersionNameComponent, v)
}

func NewTimestampComponent(t uint64) Component {
	return NewNumberComponent(TypeTimestampNameComponent, t)
}

func (c Component) IsGeneric(text string) bool {
	return c.Typ == TypeGenericNameComponent && string(c.Val) == text
}

func (c Component) IsKeyword(keyword string) bool {
	return c.Typ == TypeKeywordNameComponent && string(c.Val) == keyword
}

func (c Component) IsSegment() bool {
	return c.Typ == TypeSegmentNameComponent
}

func (c Component) IsByteOffset() bool {
	return c.Typ == TypeByteOffsetNameComponent
}

func (c Component) IsSequenceNum() bool {
	return c.Typ == TypeSequenceNumNameComponent
}

func (c Component) IsVersion() bool {
	return c.Typ == TypeVersionNameComponent
}

func (c Component) IsTimestamp() bool {
	return c.Typ == TypeTimestampNameComponent
}

// WithVersion appends a version component to the name.
func (n Name) WithVersion(v uint64) Name {
	if n.At(-1).IsVersion() {
		n = n.Prefix(-1) // pop old version
	}
	switch v {
	case VersionImmutable:
		v = 0
	case VersionUnixMicro:
		if now := time.Now().UnixMicro(); now > 0 { // > 1970
			v = uint64(now)
		} else {
			panic("current unix time is negative")
		}
	}
	return n.Append(NewVersionComponent(v))
}
