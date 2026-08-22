package codegen

import (
	"fmt"
)

// ByteField represents a pointer to the wire address of a byte.
type ByteField struct {
	BaseTlvField
}

func NewByteField(name string, typeNum uint64, annotation string, _ *TlvModel) (TlvField, error) {
	return &ByteField{
		BaseTlvField: BaseTlvField{
			name:    name,
			typeNum: typeNum,
		},
	}, nil
}

func (f *ByteField) GenEncodingLength() (string, error) {
	g := strErrBuf{}
	g.printlnf("if value.%s != nil {", f.name)
	g.printlne(GenTypeNumLen(f.typeNum))
	g.printlnf("l += 2")
	g.printlnf("}")
	return g.output()
}

func (f *ByteField) GenEncodingWirePlan() (string, error) {
	return f.GenEncodingLength()
}

func (f *ByteField) GenEncodeInto() (string, error) {
	g := strErrBuf{}
	g.printlnf("if value.%s != nil {", f.name)
	g.printlne(GenEncodeTypeNum(f.typeNum))
	g.printlnf("buf[pos] = 1")
	g.printlnf("buf[pos+1] = byte(*value.%s)", f.name)
	g.printlnf("pos += 2")
	g.printlnf("}")
	return g.output()
}

func (f *ByteField) GenReadFrom() (string, error) {
	g := strErrBuf{}
	g.printlnf("{")
	g.printlnf("buf, err := reader.ReadBuf(1)")
	g.printlnf("if err == io.EOF {")
	g.printlnf("err = io.ErrUnexpectedEOF")
	g.printlnf("}")
	g.printlnf("value.%s = &buf[0]", f.name)
	g.printlnf("}")
	return g.output()
}

func (f *ByteField) GenSkipProcess() (string, error) {
	return fmt.Sprintf("value.%s = nil", f.name), nil
}
