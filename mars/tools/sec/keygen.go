package sec

import (
	"crypto/elliptic"
	"fmt"
	"os"
	"strconv"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/security"
	sig "github.com/named-data/ndnd/std/security/signer"
	"github.com/spf13/cobra"
)

type ToolKeygen struct{}

func (t *ToolKeygen) configure(cmd *cobra.Command) {
	cmd.AddGroup(&cobra.Group{
		ID:    "key",
		Title: "Key Management",
	})

	cmd.AddCommand(&cobra.Command{
		GroupID: "key",
		Use:     "keygen IDENTITY KEY-TYPE [params]",
		Short:   "Generate a new NDN key pair",
		Args:    cobra.MinimumNArgs(2),
		Example: `  ndnd sec keygen /alice ed25519
  ndnd sec keygen /ndn/bob rsa 2048
  ndnd sec keygen /carol ecc secp256r1`,
		Run: t.keygen,
	})
}

func (t *ToolKeygen) keygen(_ *cobra.Command, args []string) {
	name, err := enc.NameFromStr(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid identity: %s\n", args[0])
		os.Exit(1)
		return
	}

	name = security.MakeKeyName(name)
	signer := t.keygenType(args[2:], name, args[1])

	data, err := sig.MarshalSecret(signer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to encode secret key: %s\n", err)
		os.Exit(1)
		return
	}

	out, err := security.PemEncode(data.Join())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to convert secret key to text: %s\n", err)
		os.Exit(1)
		return
	}

	os.Stdout.Write(out)
}

func (t *ToolKeygen) keygenType(args []string, name enc.Name, keyType string) ndn.Signer {
	switch keyType {
	case "rsa":
		return t.keygenRsa(args, name)
	case "ed25519":
		return t.keygenEd25519(args, name)
	case "ecc":
		return t.keygecEcc(args, name)
	default:
		fmt.Fprintf(os.Stderr, "Unsupported key type: %s\n", keyType)
		os.Exit(2)
		return nil
	}
}

func (t *ToolKeygen) keygenRsa(args []string, name enc.Name) ndn.Signer {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage:\n  keygen rsa <key-size>\n")
		os.Exit(2)
		return nil
	}

	keySize, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid key size: %s\n", args[0])
		os.Exit(1)
		return nil
	}

	signer, err := sig.KeygenRsa(name, keySize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate RSA key: %s\n", err)
		os.Exit(1)
		return nil
	}

	return signer
}

func (t *ToolKeygen) keygenEd25519(_ []string, name enc.Name) ndn.Signer {
	signer, err := sig.KeygenEd25519(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate Ed25519 key: %s\n", err)
		os.Exit(1)
		return nil
	}
	return signer
}

func (t *ToolKeygen) keygecEcc(args []string, name enc.Name) ndn.Signer {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage:\n  keygen ecc <curve>\n")
		fmt.Fprintf(os.Stderr, "Supported curves:\n  secp256r1, secp384r1, secp521r1\n")
		os.Exit(2)
		return nil
	}

	var curve elliptic.Curve
	switch args[0] {
	case "secp256r1":
		curve = elliptic.P256()
	case "secp384r1":
		curve = elliptic.P384()
	case "secp521r1":
		curve = elliptic.P521()
	default:
		fmt.Fprintf(os.Stderr, "Unsupported curve: %s\n", args[0])
		os.Exit(1)
		return nil
	}

	signer, err := sig.KeygenEcc(name, curve)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate EC key: %s\n", err)
		os.Exit(1)
		return nil
	}
	return signer
}
