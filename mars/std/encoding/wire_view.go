package encoding

import (
	"io"
)

// WireView is a parsing view of a Wire.
// It lives entirely on the stack and fits in a cache line.
type WireView struct {
	wire  Wire
	apos  int // absolute position from start of wire
	rpos  int // relative position within segment
	seg   int // segment index
	start int // first allowed position (absolute)
	end   int // last allowed position (absolute)
}

func NewWireView(wire Wire) WireView {
	end := 0
	for _, seg := range wire {
		end += len(seg)
	}
	return WireView{wire: wire, end: end}
}

func NewBufferView(buf Buffer) WireView {
	return NewWireView(Wire{buf})
}

func (r *WireView) IsEOF() bool {
	return r.apos >= r.end
}

func (r *WireView) Pos() int {
	return r.apos - r.start
}

func (r *WireView) Length() int {
	return r.end - r.start
}

func (r *WireView) ReadByte() (byte, error) {
	if r.IsEOF() {
		return 0, r._eof()
	}
	b := r.wire[r.seg][r.rpos]
	r.apos++
	r.rpos++
	if r.rpos == len(r.wire[r.seg]) {
		r.rpos = 0
		r.seg++
	}
	return b, nil
}

func (r *WireView) ReadFull(cpy []byte) (int, error) {
	cpypos := 0
	for cpypos < len(cpy) {
		if r.IsEOF() {
			return cpypos, r._overflow()
		}
		n := copy(cpy[cpypos:], r.wire[r.seg][r.rpos:])
		cpypos += n
		r.apos += n
		r.rpos += n
		if r.rpos == len(r.wire[r.seg]) {
			r.rpos = 0
			r.seg++
		}
	}
	return cpypos, nil
}

func (r *WireView) Skip(n int) error {
	_, err := r.SkipGetSegCount(n)
	return err
}

// _skip skips the next n bytes.
// used as utility for ReadWire to get the number of segments to read.
func (r *WireView) SkipGetSegCount(n int) (int, error) {
	segcount := 0
	left := n
	for left > 0 {
		segcount++
		if r.IsEOF() {
			return segcount, r._overflow()
		}
		segleft := len(r.wire[r.seg]) - r.rpos
		if left < segleft {
			r.apos += left
			r.rpos += left
			return segcount, nil
		} else {
			left -= segleft
			r.apos += segleft
			r.rpos = 0
			r.seg++
		}
	}
	return segcount, nil
}

func (r *WireView) ReadWire(size int) (Wire, error) {
	r_sz := *r // copy
	w_size, err := r_sz.SkipGetSegCount(size)
	if err != nil {
		return nil, err
	}

	// bounds checking is already done
	ret := make(Wire, w_size)
	for i := 0; i < w_size; i++ {
		ret[i] = r.readSeg(size)
		size -= len(ret[i])
	}

	return ret, nil
}

// reads upto size bytes from the current segment, without copying.
func (r *WireView) readSeg(size int) []byte {
	segleft := len(r.wire[r.seg]) - r.rpos
	if size < segleft {
		ret := r.wire[r.seg][r.rpos : r.rpos+size]
		r.apos += size
		r.rpos += size
		return ret
	} else {
		ret := r.wire[r.seg][r.rpos:]
		r.apos += segleft
		r.rpos = 0
		r.seg++
		return ret
	}
}

func (r *WireView) Delegate(size int) WireView {
	if size > r.end-r.apos {
		return WireView{} // invalid
	}
	ret := *r
	ret.start = ret.apos
	ret.end = ret.apos + size
	r.Skip(size)
	return ret
}

func (r *WireView) CopyN(w io.Writer, size int) (int, error) {
	written := 0
	for written < size {
		if r.IsEOF() {
			return written, r._overflow()
		}
		seg := r.readSeg(int(size) - written)
		written += len(seg)
		n, err := w.Write(seg)
		if n != len(seg) {
			return written, io.ErrShortWrite
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (r *WireView) ReadBuf(size int) ([]byte, error) {
	if size > r.end-r.apos {
		return nil, r._overflow()
	}
	if size == 0 {
		return []byte{}, nil
	}
	if r.IsEOF() {
		return []byte{}, r._eof()
	}

	// skip allocation if the entire buffer is in the current segment
	if size <= len(r.wire[r.seg])-r.rpos {
		ret := r.wire[r.seg][r.rpos : r.rpos+size]
		r.apos += size
		r.rpos += size
		if r.rpos == len(r.wire[r.seg]) {
			r.rpos = 0
			r.seg++
		}
		return ret, nil
	}

	ret := make([]byte, size)
	written := 0
	for written < size {
		seg := r.readSeg(size - written)
		copy(ret[written:], seg)
		written += len(seg)
	}
	return ret, nil
}

func (r *WireView) Range(start, end int) Wire {
	rcopy := WireView{wire: r.wire, end: r.end}
	rcopy.Skip(r.start + start)
	w, err := rcopy.ReadWire(end - start)
	if err != nil {
		return Wire{}
	}
	return w
}

// Debug prints the remaining bytes in the buffer.
func (r WireView) Debug() []byte {
	b, _ := r.ReadBuf(r.end - r.apos)
	return b
}

func (r *WireView) _eof() error {
	return io.EOF
}

func (r *WireView) _overflow() error {
	return ErrBufferOverflow
}
