package sync

import (
	"errors"
	"fmt"

	enc "github.com/named-data/ndnd/std/encoding"
)

var ErrSnapshot = errors.New("snapshot error")

type ErrSync struct {
	Publisher enc.Name
	BootTime  uint64
	Err       error
}

func (e *ErrSync) Error() string {
	return fmt.Sprintf("sync error [%s][%d]: %v", e.Publisher, e.BootTime, e.Err)
}

func (e *ErrSync) Unwrap() error {
	return e.Err
}
