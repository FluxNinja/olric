// Copyright 2018-2021 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dmap

import (
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/pkg/storage"
)

func (dm *DMap) localExpireOnReplica(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.BACKUP)
	f, err := dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}

	e.fragment = f
	f.Lock()
	defer f.Unlock()

	return dm.localExpire(e)
}

func (dm *DMap) localExpire(e *env) error {
	ttl := timeoutToTTL(e.timeout)
	entry := e.fragment.storage.NewEntry()
	entry.SetTimestamp(time.Now().UnixNano()) // TODO: Get timestamp from protocol message or user-level API.
	entry.SetTTL(ttl)
	err := e.fragment.storage.UpdateTTL(e.hkey, entry)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = ErrKeyNotFound
		}
		return err
	}
	return nil
}

func (dm *DMap) asyncExpireOnCluster(e *env) error {
	// Fire and forget mode.
	owners := dm.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		dm.s.wg.Add(1)
		go func(host discovery.Member) {
			defer dm.s.wg.Done()

			// TODO: Improve logging
			cmd := resp.NewExpire(dm.name, e.key, e.timeout.Seconds()).SetReplica().Command(dm.s.ctx)
			rc := dm.s.respClient.Get(host.String())
			err := rc.Process(dm.s.ctx, cmd)
			if err != nil {
				if dm.s.log.V(3).Ok() {
					dm.s.log.V(3).Printf("[ERROR] Failed to set expire in async mode: %v", err)
				}
				return
			}
			if cmd.Err() != nil {
				if dm.s.log.V(3).Ok() {
					dm.s.log.V(3).Printf("[ERROR] Failed to set expire in async mode: %v", err)
				}
			}
		}(owner)
	}
	return dm.putOnFragment(e)
}

func (dm *DMap) syncExpireOnCluster(e *env) error {
	// Quorum based replication.
	var successful int
	owners := dm.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		cmd := resp.NewExpire(dm.name, e.key, e.timeout.Seconds()).SetReplica().Command(dm.s.ctx)
		rc := dm.s.respClient.Get(owner.String())
		err := rc.Process(dm.s.ctx, cmd)
		// TODO: Improve logging
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
				owner, e.dmap, err)
			continue
		}
		if cmd.Err() != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
				owner, e.dmap, err)
			continue
		}
		successful++
	}
	err := dm.localExpire(e)
	if err != nil {
		if dm.s.log.V(3).Ok() {
			dm.s.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
				dm.s.rt.This(), e.dmap, err)
		}
	} else {
		successful++
	}
	if successful >= dm.s.config.WriteQuorum {
		return nil
	}
	return ErrWriteQuorum
}

func (dm *DMap) callExpireOnCluster(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.PRIMARY)
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}
	e.fragment = f
	f.Lock()
	defer f.Unlock()

	if dm.s.config.ReplicaCount == config.MinimumReplicaCount {
		// MinimumReplicaCount is 1. So it's enough to put the key locally. There is no
		// other replica host.
		return dm.localExpire(e)
	}

	switch dm.s.config.ReplicationMode {
	case config.AsyncReplicationMode:
		return dm.asyncExpireOnCluster(e)
	case config.SyncReplicationMode:
		return dm.syncExpireOnCluster(e)
	default:
		return fmt.Errorf("invalid replication mode: %v", dm.s.config.ReplicationMode)
	}
}

func (dm *DMap) expire(e *env) error {
	e.hkey = partitions.HKey(e.dmap, e.key)
	member := dm.s.primary.PartitionByHKey(e.hkey).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		// We are on the partition owner.
		return dm.callExpireOnCluster(e)
	}

	// Redirect to the partition owner
	cmd := resp.NewExpire(dm.name, e.key, e.timeout.Seconds()).Command(dm.s.ctx)
	rc := dm.s.respClient.Get(member.String())
	err := rc.Process(dm.s.ctx, cmd)
	if err != nil {
		return err
	}
	return cmd.Err()
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contain the key. It's thread-safe.
func (dm *DMap) Expire(key string, timeout time.Duration) error {
	e := newEnv()
	e.dmap = dm.name
	e.key = key
	e.timeout = timeout
	return dm.expire(e)
}
