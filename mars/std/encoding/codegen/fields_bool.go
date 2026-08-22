package codegen

import "fmt"

// BoolField represents a boolean field.
type BoolField struct {
	BaseTlvField
}

func NewBoolField(name string, typeNum uint64, _ string, _ *TlvModel) (TlvField, error) {
	return &BoolField{
		BaseTlvField: BaseTlvField{
			name:    name,
			typeNum: typeNum,
		},
	}, nil
}

func (f *BoolField) GenEncodingLength() (string, error) {
	g := strErrBuf{}
	g.printlnf("if value.%s {", f.name)
	g.printlne(GenTypeNumLen(f.typeNum))
	g.printlne(GenTypeNumLen(0))
	g.printlnf("}")
	return g.output()
}

func (f *BoolField) GenEncodingWirePlan() (string, error) {
	return f.GenEncodingLength()
}

func (f *BoolField) GenEncodeInto() (string, error) {
	g := strErrBuf{}
	g.printlnf("if value.%s {", f.name)
	g.printlne(GenEncodeTypeNum(f.typeNum))
	g.printlne(GenEncodeTypeNum(0))
	g.printlnf("}")
	return g.output()
}

func (f *BoolField) GenReadFrom() (string, error) {
	g := strErrBuf{}
	g.printlnf("value.%s = true", f.name)
	g.printlnf("err = reader.Skip(int(l))")
	return g.output()
}

func (f *BoolField) GenSkipProcess() (string, error) {
	return fmt.Sprintf("value.%s = false", f.name), nil
}
