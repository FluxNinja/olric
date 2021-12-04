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
	"context"
	"errors"
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/internal/server"
	"sync"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/locker"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/buraksezer/olric/serializer"
)

var errFragmentNotFound = errors.New("fragment not found")

type storageMap struct {
	engines map[string]storage.Engine
	configs map[string]map[string]interface{}
}

type Service struct {
	sync.RWMutex // protects dmaps map

	log        *flog.Logger
	config     *config.Config
	respClient *server.Client
	respServer *server.Server
	rt         *routingtable.RoutingTable
	serializer serializer.Serializer
	primary    *partitions.Partitions
	backup     *partitions.Partitions
	locker     *locker.Locker
	dmaps      map[string]*DMap
	operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)
	storage    *storageMap
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func registerErrors() {
	resp.SetError("NOSUCHLOCK", ErrNoSuchLock)
	resp.SetError("LOCKNOTACQUIRED", ErrLockNotAcquired)
	resp.SetError("READQUORUM", ErrReadQuorum)
	resp.SetError("WRITEQUORUM", ErrWriteQuorum)
	resp.SetError("ENDOFQUERY", ErrEndOfQuery)
	resp.SetError("DMAPNOTFOUND", ErrDMapNotFound)
	resp.SetError("KEYTOOLARGE", ErrKeyTooLarge)
	resp.SetError("KEYNOTFOUND", ErrKeyNotFound)
	resp.SetError("KEYFOUND", ErrKeyFound)
}

func NewService(e *environment.Environment) (service.Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		config:     e.Get("config").(*config.Config),
		serializer: e.Get("config").(*config.Config).Serializer,
		respClient: e.Get("respClient").(*server.Client),
		respServer: e.Get("respServer").(*server.Server),
		log:        e.Get("logger").(*flog.Logger),
		rt:         e.Get("routingtable").(*routingtable.RoutingTable),
		primary:    e.Get("primary").(*partitions.Partitions),
		backup:     e.Get("backup").(*partitions.Partitions),
		locker:     e.Get("locker").(*locker.Locker),
		storage: &storageMap{
			engines: make(map[string]storage.Engine),
			configs: make(map[string]map[string]interface{}),
		},
		dmaps:      make(map[string]*DMap),
		operations: make(map[protocol.OpCode]func(w, r protocol.EncodeDecoder)),
		ctx:        ctx,
		cancel:     cancel,
	}
	registerErrors()
	s.RegisterHandlers()
	return s, nil
}

func (s *Service) isAlive() bool {
	select {
	case <-s.ctx.Done():
		// The node is gone.
		return false
	default:
	}
	return true
}

// Start starts the distributed map service.
func (s *Service) Start() error {
	s.wg.Add(1)
	go s.janitorWorker()

	s.wg.Add(1)
	go s.compactionWorker()

	s.wg.Add(1)
	go s.evictKeysAtBackground()

	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.cancel()
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return err
		}
	case <-done:
	}
	return nil
}

var _ service.Service = (*Service)(nil)
