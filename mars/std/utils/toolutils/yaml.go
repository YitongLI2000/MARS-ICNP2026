package toolutils

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

func ReadYaml(dest any, file string) {
	f, err := os.Open(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open configuration file: %+v\n", err)
		os.Exit(3)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f, yaml.Strict())
	if err = dec.Decode(dest); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse configuration file: %+v\n", err)
		os.Exit(3)
	}
}
