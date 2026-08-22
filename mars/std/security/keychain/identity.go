package keychain

import (
	"sort"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/ndn"
)

// keyChainKey is a container for a private key and its associated certificates.
type keyChainKey struct {
	// signer is the private key object.
	signer ndn.Signer
	// uniqueCerts is the list of unique certs in this key.
	// Version number in the cert name is always set to zero to de-duplicate.
	uniqueCerts []enc.Name
	// latestCertVer is the latest certificate version.
	latestCertVer uint64
}

func (k *keyChainKey) KeyName() enc.Name {
	return k.signer.KeyName()
}

func (k *keyChainKey) Signer() ndn.Signer {
	return k.signer
}

func (k *keyChainKey) UniqueCerts() []enc.Name {
	return k.uniqueCerts
}

// insertCert adds a certificate to the key container.
func (k *keyChainKey) insertCert(certName enc.Name) {
	version := certName.At(-1).NumberVal()
	if version > k.latestCertVer {
		k.latestCertVer = version
	}

	// De-duplicate by removing the version number.
	uniqueName := certName.Prefix(-1).Append(enc.NewVersionComponent(0))
	for _, n := range k.uniqueCerts {
		if n.Equal(uniqueName) {
			return
		}
	}
	k.uniqueCerts = append(k.uniqueCerts, uniqueName)
}

// keyChainIdentity is a container for a named identity and its associated keys.
type keyChainIdentity struct {
	// name is the name of the identity.
	name enc.Name
	// keyList is the list of keyList containers for this identity.
	keyList []ndn.KeyChainKey
}

func (id *keyChainIdentity) Name() enc.Name {
	return id.name
}

func (id *keyChainIdentity) Keys() []ndn.KeyChainKey {
	return id.keyList
}

func (id *keyChainIdentity) insertCert(name enc.Name) {
	if !id.Name().IsPrefix(name) {
		return
	}
	for _, key := range id.keyList {
		if key.KeyName().IsPrefix(name) {
			key.(*keyChainKey).insertCert(name)
			id.sort()
		}
	}
}

func (id *keyChainIdentity) sort() {
	sort.Slice(id.keyList, func(i, j int) bool {
		return id.keyList[i].(*keyChainKey).latestCertVer >
			id.keyList[j].(*keyChainKey).latestCertVer
	})
}
