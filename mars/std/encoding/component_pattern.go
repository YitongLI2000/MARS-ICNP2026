package encoding

import (
	"fmt"
	"strings"
)

type Matching map[string][]byte

type ComponentPattern interface {
	// ComponentPatternTrait returns the type trait of Component or Pattern
	// This is used to make ComponentPattern a union type of Component or Pattern
	// Component | Pattern does not work because we need a mixed list NamePattern
	ComponentPatternTrait() ComponentPattern

	// String returns the string of the component, with naming conventions.
	// Since naming conventions are not standardized, this should not be used for purposes other than logging.
	// please use CanonicalString() for stable string representation.
	String() string

	// CanonicalString returns the string representation of the component without naming conventions.
	CanonicalString() string

	// Compare returns an integer comparing two components lexicographically.
	// It compares the type number first, and then its value.
	// A component is always less than a pattern.
	// The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(ComponentPattern) int

	// Equal returns the two components/patterns are the same.
	Equal(ComponentPattern) bool

	// IsMatch returns if the Component value matches with the current component/pattern.
	IsMatch(value Component) bool

	// Match matches the current pattern/component with the value, and put the matching into the Matching map.
	Match(value Component, m Matching)

	// FromMatching initiates the pattern from the Matching map.
	FromMatching(m Matching) (*Component, error)
}

func ComponentPatternFromStr(s string) (ComponentPattern, error) {
	if len(s) <= 0 || s[0] != '<' {
		return ComponentFromStr(s)
	}
	if s[len(s)-1] != '>' {
		return nil, ErrFormat{"invalid component pattern: " + s}
	}
	s = s[1 : len(s)-1]
	strs := strings.Split(s, "=")
	if len(strs) > 2 {
		return nil, ErrFormat{"too many '=' in component pattern: " + s}
	}
	if len(strs) == 2 {
		typ, _, err := parseCompTypeFromStr(strs[0])
		if err != nil {
			return nil, err
		}
		return Pattern{
			Typ: typ,
			Tag: strs[1],
		}, nil
	} else {
		return Pattern{
			Typ: TypeGenericNameComponent,
			Tag: strs[0],
		}, nil
	}
}

type Pattern struct {
	Typ TLNum
	Tag string
}

func (p Pattern) String() string {
	if p.Typ == TypeGenericNameComponent {
		return "<" + p.Tag + ">"
	} else if conv, ok := compConvByType[p.Typ]; ok {
		return "<" + conv.name + "=" + p.Tag + ">"
	} else {
		return fmt.Sprintf("<%d=%s>", p.Typ, p.Tag)
	}
}

func (p Pattern) CanonicalString() string {
	if p.Typ == TypeGenericNameComponent {
		return "<" + p.Tag + ">"
	} else {
		return fmt.Sprintf("<%d=%s>", p.Typ, p.Tag)
	}
}

func (p Pattern) ComponentPatternTrait() ComponentPattern {
	return p
}

func (p Pattern) Compare(rhs ComponentPattern) int {
	rp, ok := rhs.(Pattern)
	if !ok {
		p, ok := rhs.(*Pattern)
		if !ok {
			// Pattern is always greater than component
			return 1
		}
		rp = *p
	}
	if p.Typ != rp.Typ {
		if p.Typ < rp.Typ {
			return -1
		} else {
			return 1
		}
	}
	return strings.Compare(p.Tag, rp.Tag)
}

func (p Pattern) Equal(rhs ComponentPattern) bool {
	rp, ok := rhs.(Pattern)
	if !ok {
		p, ok := rhs.(*Pattern)
		if !ok {
			return false
		}
		rp = *p
	}
	return p.Typ == rp.Typ && p.Tag == rp.Tag
}

func (p Pattern) Match(value Component, m Matching) {
	m[p.Tag] = make([]byte, len(value.Val))
	copy(m[p.Tag], value.Val)
}

func (p Pattern) FromMatching(m Matching) (*Component, error) {
	val, ok := m[p.Tag]
	if !ok {
		return nil, ErrNotFound{p.Tag}
	}
	return &Component{
		Typ: p.Typ,
		Val: []byte(val),
	}, nil
}

func (p Pattern) IsMatch(value Component) bool {
	return p.Typ == value.Typ
}
