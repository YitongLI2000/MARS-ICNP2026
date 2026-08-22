//go:build js && wasm

package keychain

import (
	"crypto/sha256"
	"encoding/hex"
	"syscall/js"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	sig "github.com/named-data/ndnd/std/security/signer"
	jsutil "github.com/named-data/ndnd/std/utils/js"
)

// KeyChainJS is a JS-based keychain.
type KeyChainJS struct {
	mem ndn.KeyChain
	api js.Value
}

// NewKeyChainJS creates a new JS-based keychain.
// See keychain_js.ts for the interface and a sample implementation.
func NewKeyChainJS(api js.Value, pubStore ndn.Store) (ndn.KeyChain, error) {
	kc := &KeyChainJS{
		mem: NewKeyChainMem(pubStore),
		api: api,
	}

	list, err := jsutil.Await(api.Call("list"))
	if err != nil {
		return nil, err
	}

	callback := js.FuncOf(func(this js.Value, args []js.Value) any {
		err := InsertFile(kc.mem, jsutil.JsArrayToSlice(args[0]))
		if err != nil {
			log.Error(kc, "Failed to insert keychain entry", "err", err)
		}

		return nil
	})
	list.Call("forEach", callback)
	callback.Release()

	return kc, nil
}

func (kc *KeyChainJS) String() string {
	return "keychain-js"
}

func (kc *KeyChainJS) Store() ndn.Store {
	return kc.mem.Store()
}

func (kc *KeyChainJS) Identities() []ndn.KeyChainIdentity {
	return kc.mem.Identities()
}

func (kc *KeyChainJS) IdentityByName(name enc.Name) ndn.KeyChainIdentity {
	return kc.mem.IdentityByName(name)
}

func (kc *KeyChainJS) InsertKey(signer ndn.Signer) error {
	err := kc.mem.InsertKey(signer)
	if err != nil {
		return err
	}

	secret, err := sig.MarshalSecret(signer)
	if err != nil {
		return err
	}

	return kc.writeFile(secret.Join(), EXT_KEY)
}

func (kc *KeyChainJS) InsertCert(wire []byte) error {
	err := kc.mem.InsertCert(wire)
	if err != nil {
		return err
	}

	return kc.writeFile(wire, EXT_CERT)
}

func (kc *KeyChainJS) writeFile(wire []byte, ext string) error {
	hash := sha256.Sum256(wire)
	filename := hex.EncodeToString(hash[:])

	kc.api.Call("write", filename+ext, jsutil.SliceToJsArray(wire))
	return nil
}
