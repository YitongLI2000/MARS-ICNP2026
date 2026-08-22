package signer

import (
	"crypto/hmac"
	"crypto/sha256"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/ndn"
)

// hmacSigner is a Data signer that uses a provided HMAC key.
type hmacSigner struct {
	key []byte
}

func (signer *hmacSigner) Type() ndn.SigType {
	return ndn.SignatureHmacWithSha256
}

func (*hmacSigner) KeyName() enc.Name {
	return nil
}

func (*hmacSigner) KeyLocator() enc.Name {
	return nil
}

func (*hmacSigner) EstimateSize() uint {
	return 32
}

func (signer *hmacSigner) Sign(covered enc.Wire) ([]byte, error) {
	mac := hmac.New(sha256.New, signer.key)
	for _, buf := range covered {
		_, err := mac.Write(buf)
		if err != nil {
			return nil, enc.ErrUnexpected{Err: err}
		}
	}
	return mac.Sum(nil), nil
}

func (*hmacSigner) Public() ([]byte, error) {
	return nil, ndn.ErrNoPubKey
}

// NewHmacSigner creates a Data signer that uses DigestSha256.
func NewHmacSigner(key []byte) ndn.Signer {
	return &hmacSigner{key}
}

// ValidateHmac verifies the signature with a known HMAC shared key.
func ValidateHmac(sigCovered enc.Wire, sig ndn.Signature, key []byte) bool {
	if sig.SigType() != ndn.SignatureHmacWithSha256 {
		return false
	}
	mac := hmac.New(sha256.New, []byte(key))
	for _, buf := range sigCovered {
		_, err := mac.Write(buf)
		if err != nil {
			return false
		}
	}
	return hmac.Equal(mac.Sum(nil), sig.SigValue())
}
