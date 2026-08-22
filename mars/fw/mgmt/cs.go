/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2022 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package mgmt

import (
	"github.com/named-data/ndnd/fw/core"
	"github.com/named-data/ndnd/fw/dispatch"
	"github.com/named-data/ndnd/fw/fw"
	"github.com/named-data/ndnd/fw/table"
	enc "github.com/named-data/ndnd/std/encoding"
	mgmt "github.com/named-data/ndnd/std/ndn/mgmt_2022"
	"github.com/named-data/ndnd/std/types/optional"
)

// ContentStoreModule is the module that handles Content Store Management.
type ContentStoreModule struct {
	manager *Thread
}

func (c *ContentStoreModule) String() string {
	return "mgmt-cs"
}

func (c *ContentStoreModule) registerManager(manager *Thread) {
	c.manager = manager
}

func (c *ContentStoreModule) getManager() *Thread {
	return c.manager
}

func (c *ContentStoreModule) handleIncomingInterest(interest *Interest) {
	// Only allow from /localhost
	if !LOCAL_PREFIX.IsPrefix(interest.Name()) {
		core.Log.Warn(c, "Received CS management Interest from non-local source")
		return
	}

	// Dispatch by verb
	verb := interest.Name()[len(LOCAL_PREFIX)+1].String()
	switch verb {
	case "config":
		c.config(interest)
	case "erase":
		// TODO
		//c.erase(interest)
	case "info":
		c.info(interest)
	default:
		core.Log.Warn(c, "Received Interest for non-existent verb", "verb", verb)
		c.manager.sendCtrlResp(interest, 501, "Unknown verb", nil)
		return
	}
}

func (c *ContentStoreModule) config(interest *Interest) {
	if len(interest.Name()) < len(LOCAL_PREFIX)+3 {
		// Name not long enough to contain ControlParameters
		core.Log.Warn(c, "Missing ControlParameters", "name", interest.Name())
		c.manager.sendCtrlResp(interest, 400, "ControlParameters is incorrect", nil)
		return
	}

	params := decodeControlParameters(c, interest)
	if params == nil {
		c.manager.sendCtrlResp(interest, 400, "ControlParameters is incorrect", nil)
		return
	}

	if (!params.Flags.IsSet() && params.Mask.IsSet()) || (params.Flags.IsSet() && !params.Mask.IsSet()) {
		core.Log.Warn(c, "Flags and Mask fields must either both be present or both be not present")
		c.manager.sendCtrlResp(interest, 409, "ControlParameters are incorrect", nil)
		return
	}

	if capacity, ok := params.Capacity.Get(); ok {
		core.Log.Info(c, "Setting CS capacity", "capacity", capacity)
		table.CfgSetCsCapacity(int(capacity))
	}

	if params.Mask.IsSet() && params.Flags.IsSet() {
		mask := params.Mask.Unwrap()
		flags := params.Flags.Unwrap()

		if mask&mgmt.CsEnableAdmit > 0 {
			val := flags&mgmt.CsEnableAdmit > 0
			core.Log.Info(c, "Setting CS admit flag", "value", val)
			table.CfgSetCsAdmit(val)
		}

		if mask&mgmt.CsEnableServe > 0 {
			val := flags&mgmt.CsEnableServe > 0
			core.Log.Info(c, "Setting CS serve flag", "value", val)
			table.CfgSetCsServe(val)
		}
	}

	c.manager.sendCtrlResp(interest, 200, "OK", &mgmt.ControlArgs{
		Capacity: optional.Some(uint64(table.CfgCsCapacity())),
		Flags:    optional.Some(c.getFlags()),
	})
}

func (c *ContentStoreModule) info(interest *Interest) {
	if len(interest.Name()) > len(LOCAL_PREFIX)+2 {
		// Ignore because contains version and/or segment components
		return
	}

	// Generate new dataset
	status := mgmt.CsInfoMsg{
		CsInfo: &mgmt.CsInfo{
			Capacity:   uint64(table.CfgCsCapacity()),
			Flags:      c.getFlags(),
			NCsEntries: 0,
		},
	}
	for threadID := 0; threadID < fw.CfgNumThreads(); threadID++ {
		thread := dispatch.GetFWThread(threadID)
		counters := thread.Counters()

		status.CsInfo.NCsEntries += uint64(counters.NCsEntries)
		status.CsInfo.NHits += uint64(counters.NCsHits)
		status.CsInfo.NMisses += uint64(counters.NCsMisses)
	}

	name := LOCAL_PREFIX.
		Append(enc.NewGenericComponent("cs")).
		Append(enc.NewGenericComponent("info"))
	c.manager.sendStatusDataset(interest, name, status.Encode())
}

func (c *ContentStoreModule) getFlags() uint64 {
	flags := uint64(0)
	if table.CfgCsAdmit() {
		flags |= mgmt.CsEnableAdmit
	}
	if table.CfgCsServe() {
		flags |= mgmt.CsEnableServe
	}
	return flags
}
