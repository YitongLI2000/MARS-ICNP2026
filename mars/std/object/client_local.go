package object

import (
	"fmt"

	enc "github.com/named-data/ndnd/std/encoding"
	rdr "github.com/named-data/ndnd/std/ndn/rdr_2024"
	spec "github.com/named-data/ndnd/std/ndn/spec_2022"
)

// LatestLocal returns the latest version name of an object in the store
func (c *Client) LatestLocal(name enc.Name) (enc.Name, error) {
	raw, err := c.store.Get(name.Append(enc.NewKeywordComponent(rdr.MetadataKeyword)), true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}

	data, _, err := spec.Spec{}.ReadData(enc.NewBufferView(raw))
	if err != nil {
		return nil, err
	}

	version := data.Name().At(-2)
	if !version.IsVersion() {
		return nil, fmt.Errorf("invalid metadata for %s", name)
	}

	return name.Append(version), nil
}

// GetLocal returns the object data from the store
func (c *Client) GetLocal(name enc.Name) (enc.Wire, error) {
	if !name.At(-1).IsVersion() {
		return nil, fmt.Errorf("GetLocal called without version (use LatestLocal): %s", name)
	}

	var wire enc.Wire
	name = name.Append(enc.NewSegmentComponent(0))
	lastSeg := uint64(0)

	for i := uint64(0); i <= lastSeg; i++ {
		name[len(name)-1] = enc.NewSegmentComponent(i)

		raw, err := c.store.Get(name, false)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, fmt.Errorf("missing segment %d for %s", i, name)
		}

		data, _, err := spec.Spec{}.ReadData(enc.NewBufferView(raw))
		if err != nil {
			return nil, err
		}

		if i == 0 {
			if fbId, ok := data.FinalBlockID().Get(); !ok {
				return nil, fmt.Errorf("missing final block id for %s", name)
			} else {
				lastSeg = fbId.NumberVal()
				wire = make(enc.Wire, 0, (lastSeg + 1))
			}
		}

		wire = append(wire, data.Content()...)
	}

	return wire, nil
}
