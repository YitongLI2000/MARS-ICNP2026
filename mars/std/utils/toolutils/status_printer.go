package toolutils

import (
	"fmt"
	"os"
	"strings"
)

type StatusPrinter struct {
	File    *os.File
	Padding int
}

func (s StatusPrinter) Print(key string, value any) {
	fmt.Fprintf(s.File, "%s%s=%v\n", strings.Repeat(" ", s.Padding-len(key)), key, value)
}
