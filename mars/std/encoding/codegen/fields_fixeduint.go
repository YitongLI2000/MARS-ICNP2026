package codegen

import (
	"fmt"
	"strings"
	"text/template"
)

// FixedUintField represents a fixed-length unsigned integer.
type FixedUintField struct {
	BaseTlvField

	opt bool
	l   uint
}

func NewFixedUintField(name string, typeNum uint64, annotation string, _ *TlvModel) (TlvField, error) {
	if annotation == "" {
		return nil, ErrInvalidField
	}
	strs := strings.Split(annotation, ":")
	optional := false
	if len(strs) >= 2 && strs[1] == "optional" {
		optional = true
	}
	l := uint(0)
	switch strs[0] {
	case "byte":
		l = 1
	case "uint16":
		l = 2
	case "uint32":
		l = 4
	case "uint64":
		l = 8
	}
	return &FixedUintField{
		BaseTlvField: BaseTlvField{
			name:    name,
			typeNum: typeNum,
		},
		opt: optional,
		l:   l,
	}, nil
}

func (f *FixedUintField) GenEncodingLength() (string, error) {
	g := strErrBuf{}
	if f.opt {
		g.printlnf("if value.%s.IsSet() {", f.name)
		g.printlne(GenTypeNumLen(f.typeNum))
		g.printlnf("l += 1 + %d", f.l)
		g.printlnf("}")
	} else {
		g.printlne(GenTypeNumLen(f.typeNum))
		g.printlnf("l += 1 + %d", f.l)
	}
	return g.output()
}

func (f *FixedUintField) GenEncodingWirePlan() (string, error) {
	return f.GenEncodingLength()
}

func (f *FixedUintField) GenEncodeInto() (string, error) {
	g := strErrBuf{}

	gen := func(name string) (string, error) {
		gi := strErrBuf{}
		switch f.l {
		case 1:
			gi.printlnf("buf[pos] = 1")
			gi.printlnf("buf[pos+1] = byte(%s)", name)
			gi.printlnf("pos += %d", 2)
		case 2:
			gi.printlnf("buf[pos] = 2")
			gi.printlnf("binary.BigEndian.PutUint16(buf[pos+1:], uint16(%s))", name)
			gi.printlnf("pos += %d", 3)
		case 4:
			gi.printlnf("buf[pos] = 4")
			gi.printlnf("binary.BigEndian.PutUint32(buf[pos+1:], uint32(%s))", name)
			gi.printlnf("pos += %d", 5)
		case 8:
			gi.printlnf("buf[pos] = 8")
			gi.printlnf("binary.BigEndian.PutUint64(buf[pos+1:], uint64(%s))", name)
			gi.printlnf("pos += %d", 9)
		}
		return gi.output()
	}

	if f.opt {
		g.printlnf("if optval, ok := value.%s.Get(); ok {", f.name)
		g.printlne(GenEncodeTypeNum(f.typeNum))
		g.printlne(gen("optval"))
		g.printlnf("}")
	} else {
		g.printlne(GenEncodeTypeNum(f.typeNum))
		g.printlne(gen("value." + f.name))
	}

	return g.output()
}

func (f *FixedUintField) GenReadFrom() (string, error) {
	g := strErrBuf{}
	digit := ""
	switch f.l {
	case 1:
		digit = "byte"
	case 2:
		digit = "uint16"
	case 4:
		digit = "uint32"
	case 8:
		digit = "uint64"
	}

	gen := func(name string) {
		if f.l == 1 {
			g.printlnf("%s, err = reader.ReadByte()", name)
			g.printlnf("if err == io.EOF {")
			g.printlnf("err = io.ErrUnexpectedEOF")
			g.printlnf("}")
		} else {
			const Temp = `{{.Name}} = {{.Digit}}(0)
			{
				for i := 0; i < int(l); i++ {
					x := byte(0)
					x, err = reader.ReadByte()
					if err != nil {
						if err == io.EOF {
							err = io.ErrUnexpectedEOF
						}
						break
					}
					{{.Name}} = {{.Digit}}({{.Name}}<<8) | {{.Digit}}(x)
				}
			}
			`
			t := template.Must(template.New("FixedUintDecode").Parse(Temp))
			g.executeTemplate(t, struct {
				Name  string
				Digit string
			}{
				Name:  name,
				Digit: digit,
			})
		}
	}

	if f.opt {
		g.printlnf("{")
		g.printlnf("optval := %s(0)", digit)
		gen("optval")
		g.printlnf("value.%s.Set(optval)", f.name)
		g.printlnf("}")
	} else {
		gen("value." + f.name)
	}
	return g.output()
}

func (f *FixedUintField) GenSkipProcess() (string, error) {
	if f.opt {
		return fmt.Sprintf("value.%s.Unset()", f.name), nil
	} else {
		return fmt.Sprintf("err = enc.ErrSkipRequired{Name: \"%s\", TypeNum: %d}", f.name, f.typeNum), nil
	}
}
