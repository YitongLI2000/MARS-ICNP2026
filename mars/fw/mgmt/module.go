/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2021 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package mgmt

import (
	spec "github.com/named-data/ndnd/std/ndn/spec_2022"
	"github.com/named-data/ndnd/std/types/optional"
)

// Module represents a management module
type Module interface {
	String() string
	registerManager(manager *Thread)
	getManager() *Thread
	handleIncomingInterest(interest *Interest)
}

type Interest struct {
	spec.Interest
	pitToken []byte
	inFace   optional.Optional[uint64]
}
