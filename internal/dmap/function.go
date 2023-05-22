package dmap

import (
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
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
	return dm.function(e)
}

func (dm *DMap) function(e *env) ([]byte, error) {
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
	return resp.Value(), nil
}

func (dm *DMap) functionOnCluster(f *env) ([]byte, error) {
  function, ok := dm.config.functions[f.function]
  if !ok {
    return nil, fmt.Errorf("function: %s is not registered", f.function)
  }

	atomicKey := f.dmap + f.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", f.key, f.dmap, err)
		}
	}()

	var currentState []byte
	var ttl int64
	entry, err := dm.getOnCluster(f.hkey, f.key)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			dm.s.log.V(3).Printf("[ERROR] Failed to get key: %s on DMap: %s: %v", f.key, f.dmap, err)
			return nil, err
		}
	} else {
		currentState = entry.Value()
		ttl = entry.TTL()
	}

	newState, result, err := function(f.key, currentState, f.value)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to call function: %s on DMap: %s: %v", f.function, f.dmap, err)
		return nil, err
	}

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
	if ttl != 0 {
		p.timeout = time.Until(time.UnixMilli(ttl))
		p.opcode = protocol.OpPutEx
	}
	err = dm.putOnCluster(p)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to put the entry after function call: %v", err)
		return nil, err
	}

	return result, nil
}
