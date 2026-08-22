package security

import (
	"crypto/rand"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/ndn"
)

// MakeKeyName generates a new key name for a given identity.
func MakeKeyName(name enc.Name) enc.Name {
	keyId := make([]byte, 8)
	rand.Read(keyId)

	return name.
		Append(enc.NewGenericComponent("KEY")).
		Append(enc.NewGenericBytesComponent(keyId))
}

// GetIdentityFromKeyName extracts the identity name from a key name.
func GetIdentityFromKeyName(name enc.Name) (enc.Name, error) {
	if name.At(-2).String() != "KEY" {
		return nil, ndn.ErrInvalidValue{Item: "KEY component"}
	}
	return name.Prefix(-2), nil
}

// MakeCertName generates a new certificate name for a given key name.
func MakeCertName(keyName enc.Name, issuerId enc.Component, version uint64) (enc.Name, error) {
	_, err := GetIdentityFromKeyName(keyName) // Check if key name is valid
	if err != nil {
		return nil, err
	}
	return keyName.Append(issuerId, enc.NewVersionComponent(version)), nil
}

// GetKeyNameFromCertName extracts the key name from a certificate name.
func GetKeyNameFromCertName(name enc.Name) (enc.Name, error) {
	if name.At(-1).Typ == enc.TypeImplicitSha256DigestComponent {
		name = name.Prefix(-1)
	}
	if name.At(-4).String() != "KEY" {
		return nil, ndn.ErrInvalidValue{Item: "KEY component"}
	}
	return name.Prefix(-2), nil
}

// GetIdentityFromCertName extracts the identity name from a certificate name.
func GetIdentityFromCertName(name enc.Name) (enc.Name, error) {
	keyName, err := GetKeyNameFromCertName(name)
	if err != nil {
		return nil, err
	}
	return GetIdentityFromKeyName(keyName)
}
