//go:build wasm

/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2021 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package impl

import (
	"syscall"
)

func SyscallReuseAddr(network string, address string, c syscall.RawConn) error {
	return nil
}

func SyscallGetSocketSendQueueSize(c syscall.RawConn) uint64 {
	return 0
}
