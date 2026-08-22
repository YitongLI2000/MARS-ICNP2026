package security

import (
	"encoding/pem"
	"fmt"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"

	spec "github.com/named-data/ndnd/std/ndn/spec_2022"
)

const PEM_TYPE_CERT = "NDN CERT"
const PEM_TYPE_SECRET = "NDN KEY"

const PEM_HEADER_NAME = "Name"
const PEM_HEADER_VALIDITY = "Validity"
const PEM_HEADER_SIGTYPE = "SigType"
const PEM_HEADER_KEY = "SignerKey"

// PemEncode converts an NDN data to a text representation following RFC 7468.
func PemEncode(raw []byte) ([]byte, error) {
	data, _, err := spec.Spec{}.ReadData(enc.NewBufferView(raw))
	if err != nil {
		return nil, err
	}

	contentType, ok := data.ContentType().Get()
	if !ok {
		return nil, ndn.ErrInvalidValue{Item: "content type"}
	}

	if data.Signature() == nil {
		return nil, ndn.ErrInvalidValue{Item: "signature"}
	}

	// Explanatory text before the block
	headers := map[string]string{
		PEM_HEADER_NAME: data.Name().String(),
	}

	// Add validity period
	if nb, na := data.Signature().Validity(); nb.IsSet() && na.IsSet() {
		headers[PEM_HEADER_VALIDITY] = fmt.Sprintf("%s - %s", nb.Unwrap(), na.Unwrap())
	}

	// Add signature type
	headers[PEM_HEADER_SIGTYPE] = data.Signature().SigType().String()

	// Add signing key for certificates
	if k := data.Signature().KeyName(); k != nil && contentType == ndn.ContentTypeKey {
		headers[PEM_HEADER_KEY] = k.String()
	}

	// Choose PEM type based on content type
	var pemType string
	switch contentType {
	case ndn.ContentTypeKey:
		pemType = PEM_TYPE_CERT
	case ndn.ContentTypeSigningKey:
		pemType = PEM_TYPE_SECRET
	default:
		return nil, fmt.Errorf("unsupported content type")
	}

	return pem.EncodeToMemory(&pem.Block{
		Type:    pemType,
		Headers: headers,
		Bytes:   raw,
	}), nil
}

// PemDecode converts a text representation of an NDN data.
func PemDecode(str []byte) [][]byte {
	ret := make([][]byte, 0)

	for {
		block, rest := pem.Decode(str)
		if block == nil {
			break
		}
		str = rest

		if block.Type != PEM_TYPE_CERT && block.Type != PEM_TYPE_SECRET {
			log.Warn(nil, "Unsupported PEM type", "type", block.Type)
			continue
		}

		ret = append(ret, block.Bytes)
	}

	return ret
}
