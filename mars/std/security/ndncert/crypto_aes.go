package ndncert

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"
	"math"

	"github.com/named-data/ndnd/std/security/ndncert/tlv"
)

const AeadSizeNonce = 12
const AeadSizeTag = 16
const AeadSizeRand = 8

type AeadMessage struct {
	IV         [AeadSizeNonce]byte
	AuthTag    [AeadSizeTag]byte
	CipherText []byte
}

func (m *AeadMessage) TLV() *tlv.CipherMsg {
	return &tlv.CipherMsg{
		InitVec:  m.IV[:],
		AuthNTag: m.AuthTag[:],
		Payload:  m.CipherText,
	}
}

func (m *AeadMessage) FromTLV(t *tlv.CipherMsg) {
	m.IV = [AeadSizeNonce]byte(t.InitVec)
	m.AuthTag = [AeadSizeTag]byte(t.AuthNTag)
	m.CipherText = t.Payload
}

type AeadCounter struct {
	block  uint32
	random [AeadSizeRand]byte
}

func NewAeadCounter() *AeadCounter {
	randomBytes := make([]byte, AeadSizeRand)
	if _, randReadErr := io.ReadFull(rand.Reader, randomBytes); randReadErr != nil {
		panic(randReadErr.Error())
	}
	return &AeadCounter{
		block:  0,
		random: [AeadSizeRand]byte(randomBytes),
	}
}

func AeadEncrypt(
	key [AeadSizeTag]byte,
	plaintext []byte,
	info []byte,
	counter *AeadCounter,
) (*AeadMessage, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Make initialization vector
	counter.block += uint32(math.Ceil(float64(float32(len(plaintext)) / float32(AeadSizeTag))))
	cblock := make([]byte, 4)
	binary.LittleEndian.PutUint32(cblock, counter.block)
	iv := append(counter.random[:], cblock...)

	// Encrypt and seal
	output := aesgcm.Seal(nil, iv, plaintext, info)

	return &AeadMessage{
		IV:         [AeadSizeNonce]byte(iv),
		AuthTag:    ([AeadSizeTag]byte)(output[len(plaintext):]),
		CipherText: output[:len(plaintext)],
	}, nil
}

func AeadDecrypt(
	key [AeadSizeTag]byte,
	message AeadMessage,
	info []byte,
) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	nonce := message.IV[:]
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	ciphertext := append(message.CipherText, message.AuthTag[:]...)

	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, info[:])
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
