package encoding_test

import (
	"crypto/rand"
	"encoding/binary"
	"runtime"
	"testing"

	enc "github.com/named-data/ndnd/std/encoding"
	tu "github.com/named-data/ndnd/std/utils/testutils"
)

func randomNames(count int, size int) []enc.Name {
	names := make([]enc.Name, count)
	for i := 0; i < count; i++ {
		for j := 0; j < size; j++ {
			bytes := make([]byte, 12+j)
			rand.Read(bytes)
			typ := max(enc.TLNum(uint16(binary.BigEndian.Uint16(bytes[:4])-1024)), 1024)
			names[i] = append(names[i], enc.NewBytesComponent(typ, bytes[4:]))
		}
	}
	return names
}

func benchmarkNameEncode(b *testing.B, size int, encode func(name enc.Name)) {
	runtime.GC()
	names := randomNames(b.N, size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encode(names[i])
	}
}

func BenchmarkNameHash(b *testing.B) {
	benchmarkNameEncode(b, 20, func(name enc.Name) { _ = name.Hash() })
}

func BenchmarkNameHashPrefix(b *testing.B) {
	benchmarkNameEncode(b, 20, func(name enc.Name) { _ = name.PrefixHash() })
}

func BenchmarkNameStringEncode(b *testing.B) {
	benchmarkNameEncode(b, 20, func(name enc.Name) { _ = name.String() })
}

func BenchmarkNameTlvStrEncode(b *testing.B) {
	benchmarkNameEncode(b, 20, func(name enc.Name) { _ = name.TlvStr() })
}

func BenchmarkNameBytesEncode(b *testing.B) {
	benchmarkNameEncode(b, 20, func(name enc.Name) { _ = name.Bytes() })
}

func BenchmarkNameComponentStringEncode(b *testing.B) {
	benchmarkNameEncode(b, 1, func(name enc.Name) { _ = name[0].String() })
}

func BenchmarkNameComponentTlvStrEncode(b *testing.B) {
	benchmarkNameEncode(b, 1, func(name enc.Name) { _ = name[0].TlvStr() })
}

func BenchmarkNameClone(b *testing.B) {
	benchmarkNameEncode(b, 20, func(name enc.Name) { _ = name.Clone() })
}

func benchmarkNameDecode[T any](b *testing.B, size int, encode func(name enc.Name) T, decode func(e T)) {
	names := randomNames(b.N, size)
	nameEncs := make([]T, b.N)
	for i := 0; i < b.N; i++ {
		nameEncs[i] = encode(names[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decode(nameEncs[i])
	}
}

func BenchmarkNameStringDecode(b *testing.B) {
	benchmarkNameDecode(b, 20,
		func(name enc.Name) string { return name.String() },
		func(s string) { _ = tu.NoErrB(enc.NameFromStr(s)) })
}

func BenchmarkNameTlvStrDecode(b *testing.B) {
	benchmarkNameDecode(b, 20,
		func(name enc.Name) string { return name.TlvStr() },
		func(s string) { _ = tu.NoErrB(enc.NameFromTlvStr(s)) })
}

func BenchmarkNameComponentStringDecode(b *testing.B) {
	benchmarkNameDecode(b, 1,
		func(name enc.Name) string { return name[0].String() },
		func(s string) { _ = tu.NoErrB(enc.ComponentFromStr(s)) })
}

func BenchmarkNameComponentTlvStrDecode(b *testing.B) {
	benchmarkNameDecode(b, 1,
		func(name enc.Name) string { return name[0].TlvStr() },
		func(s string) { _ = tu.NoErrB(enc.ComponentFromTlvStr(s)) })
}
