package signer

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/ndn"
)

// ValidateData verifies the signature of a Data packet with a certificate.
func ValidateData(data ndn.Data, sigCovered enc.Wire, cert ndn.Data) (bool, error) {
	switch data.Signature().SigType() {
	case ndn.SignatureSha256WithRsa:
		pkey, err := x509.ParsePKIXPublicKey(cert.Content().Join())
		if err != nil {
			return false, err
		}
		if pub, ok := pkey.(*rsa.PublicKey); ok {
			return ValidateRsa(sigCovered, data.Signature(), pub), nil
		}
	case ndn.SignatureSha256WithEcdsa:
		pkey, err := x509.ParsePKIXPublicKey(cert.Content().Join())
		if err != nil {
			return false, err
		}
		if pub, ok := pkey.(*ecdsa.PublicKey); ok {
			return validateEcdsa(sigCovered, data.Signature(), pub), nil
		}
	case ndn.SignatureEd25519:
		pkey, err := x509.ParsePKIXPublicKey(cert.Content().Join())
		if err != nil {
			return false, err
		}
		if pub, ok := pkey.(ed25519.PublicKey); ok {
			return validateEd25519(sigCovered, data.Signature(), pub), nil
		}
	}

	return false, ndn.ErrInvalidValue{
		Item:  "Signature.SigType",
		Value: data.Signature().SigType(),
	}
}
