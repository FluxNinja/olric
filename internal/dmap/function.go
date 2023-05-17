package dmap

import (
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/storage"
)

func (dm *DMap) Function(key string, function string, arg []byte) ([]byte, error) {
	e := &env{
		opcode:    protocol.OpFunction,
		dmap:      dm.name,
		key:       key,
		timestamp: time.Now().UnixNano(),
		kind:      partitions.PRIMARY,
		function:  function,
		value:     arg,
	}
	entry, err := dm.function(e)
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

func (dm *DMap) function(e *env) (storage.Entry, error) {
	e.hkey = partitions.HKey(dm.name, e.key)
	member := dm.s.primary.PartitionByHKey(e.hkey).Owner()
	// We are on the partition owner. So we can call the function directly.
	if member.CompareByName(dm.s.rt.This()) {
		return dm.functionOnCluster(e)
	}

	// Redirect to the partition owner.
	req := e.toReq(e.opcode)
	resp, err := dm.s.requestTo(member.String(), req)
	if err != nil {
		return nil, err
	}
	entry := dm.engine.NewEntry()
	entry.Decode(resp.Value())
	return entry, err
}

func (dm *DMap) functionOnCluster(f *env) (storage.Entry, error) {
	atomicKey := f.dmap + f.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", f.key, f.dmap, err)
		}
	}()

	if dm.config.functions[f.function] == nil {
		return nil, fmt.Errorf("function: %s is not registered", f.function)
	}

	var currentState []byte
	entry, err := dm.getOnCluster(f.hkey, f.key)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			dm.s.log.V(3).Printf("[ERROR] Failed to get key: %s on DMap: %s: %v", f.key, f.dmap, err)
			return nil, err
		}
	} else {
		currentState = entry.Value()
	}

	newState := dm.config.functions[f.function](f.key, currentState, f.value)

	// Put
	p := &env{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          dm.name,
		key:           f.key,
		hkey:          f.hkey,
		timestamp:     f.timestamp,
		kind:          partitions.PRIMARY,
		value:         newState,
	}

	err = dm.putOnCluster(p)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to put the entry after function call: %v", err)
		return nil, err
	}

	// get the entry again
	entry, err = dm.getOnCluster(f.hkey, f.key)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to get the entry after function call: %v", err)
		return nil, err
	}

	return entry, nil
}
