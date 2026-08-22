package dvc

import (
	"fmt"
	"os"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/tools/nfdc"
	"github.com/spf13/cobra"
)

func (t *Tool) RunDvLinkCreate(cmd *cobra.Command, args []string) {
	t.Start()
	defer t.Stop()

	// Get router status to get network name
	status, err := t.DvStatus()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get router status: %v\n", err)
		os.Exit(1)
	}

	// /localhop/<network>/32=DV/32=ADS/32=ACT
	name := enc.LOCALHOP.
		Append(status.NetworkName.Name...).
		Append(enc.NewKeywordComponent("DV")).
		Append(enc.NewKeywordComponent("ADS")).
		Append(enc.NewKeywordComponent("ACT"))

	new(nfdc.Tool).ExecCmd(cmd, "rib", "register", []string{
		"persistency=permanent",
		fmt.Sprintf("face=%s", args[0]),
		fmt.Sprintf("prefix=%s", name),
	}, []string{})
}

func (t *Tool) RunDvLinkDestroy(cmd *cobra.Command, args []string) {
	// just destroy the face assuming we created it
	new(nfdc.Tool).ExecCmd(cmd, "faces", "destroy", []string{
		fmt.Sprintf("face=%s", args[0]),
	}, []string{})
}
