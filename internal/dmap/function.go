package dmap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
)

func (dm *DMap) Function(ctx context.Context, key string, function string, arg []byte) ([]byte, error) {
	hkey := partitions.HKey(dm.name, key)
	member := dm.s.primary.PartitionByHKey(hkey).Owner()

	// We are on the partition owner. So we can call the function directly.
	if member.CompareByName(dm.s.rt.This()) {
		return dm.functionOnCluster(dm.name, hkey, key, function, arg)
	}

	// Redirect to the partition owner.
	cmd := protocol.NewFunction(dm.name, key, function, arg).Command(dm.s.ctx)
	rc := dm.s.client.Get(member.String())
	err := rc.Process(ctx, cmd)
	if err != nil {
		return nil, protocol.ConvertError(err)
	}

	value, err := cmd.Bytes()
	if err != nil {
		return nil, protocol.ConvertError(err)
	}

	return value, protocol.ConvertError(cmd.Err())
}

func (dm *DMap) functionOnCluster(dmap string, hkey uint64, key string, function string, arg []byte) ([]byte, error) {
	f, ok := dm.config.functions[function]
	if !ok {
		return nil, fmt.Errorf("function: %s is not registered", function)
	}

	atomicKey := dmap + key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", key, dmap, err)
		}
	}()

	var currentState []byte
	var ttl int64
	entry, err := dm.getOnCluster(hkey, key)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			dm.s.log.V(3).Printf("[ERROR] Failed to get key: %s on DMap: %s: %v", key, dmap, err)
			return nil, err
		}
	} else {
		currentState = entry.Value()
		ttl = entry.TTL()
	}

	newState, result, err := f(key, currentState, arg)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to call function: %s on DMap: %s: %v", function, dmap, err)
		return nil, err
	}

	p := &env{
		function:  function,
		dmap:      dm.name,
		key:       key,
		hkey:      hkey,
		timestamp: time.Now().UnixNano(),
		kind:      partitions.PRIMARY,
		value:     newState,
		putConfig: &PutConfig{},
	}
	if ttl != 0 {
		p.timeout = time.Until(time.UnixMilli(ttl))
		p.putConfig.HasPX = true
		p.putConfig.PX = time.Until(time.UnixMilli(ttl))
	}
	err = dm.putOnCluster(p)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to put the entry after function call: %v", err)
		return nil, err
	}

	return result, nil
}
