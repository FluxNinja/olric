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
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"sync"
	"sync/atomic"
	"testing"
)

func TestDMap_Atomic_Incr(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "incr"

	incr := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.Incr(key, 1)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Incr: %v", err)
			return
		}
	}

	dm, err := s.NewDMap("atomic_test")
	require.NoError(t, err)

	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go incr(dm)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(key)
	require.NoError(t, err)
	require.Equal(t, 100, res)
}

func TestDMap_Atomic_Decr(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "decr"

	decr := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.Decr(key, 1)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
	}

	dm, err := s.NewDMap("atomic_test")
	require.NoError(t, err)

	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go decr(dm)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(key)
	require.NoError(t, err)

	if res.(int) != -100 {
		t.Fatalf("Expected 100. Got: %v", res)
	}
}

func TestDMap_Atomic_GetPut(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var total int64
	var wg sync.WaitGroup
	var start chan struct{}
	key := "getput"
	getput := func(dm *DMap, i int) {
		<-start
		defer wg.Done()

		oldval, err := dm.GetPut(key, i)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
		if oldval != nil {
			atomic.AddInt64(&total, int64(oldval.(int)))
		}
	}

	dm, err := s.NewDMap("atomic_test")
	require.NoError(t, err)

	start = make(chan struct{})
	var final int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go getput(dm, i)
		final += int64(i)
	}
	close(start)
	wg.Wait()

	last, err := dm.Get(key)
	require.NoError(t, err)

	atomic.AddInt64(&total, int64(last.(int)))
	if atomic.LoadInt64(&total) != final {
		t.Fatalf("Expected %d. Got: %d", final, atomic.LoadInt64(&total))
	}
}

func TestDMap_incrCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			cmd := resp.NewIncr("mydmap", "mykey", 1).Command(context.Background())
			rc := s.respClient.Get(s.rt.This().String())
			err := rc.Process(context.Background(), cmd)
			if err != nil {
				return err
			}
			_, err = cmd.Result()
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	cmd := resp.NewGet("mydmap", "mykey").Command(context.Background())
	rc := s.respClient.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)

	value, err := cmd.Bytes()
	require.NoError(t, err)

	var v interface{}
	err = s.serializer.Unmarshal(value, &v)
	require.NoError(t, err)
	require.Equal(t, 100, v.(int))
}

func TestDMap_incrCommandHandler_Single_Request(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := resp.NewIncr("mydmap", "mykey", 100).Command(context.Background())
	rc := s.respClient.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)
	value, err := cmd.Result()

	require.NoError(t, err)
	require.Equal(t, 100, int(value))
}

func TestDMap_decrCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			cmd := resp.NewDecr("mydmap", "mykey", 1).Command(context.Background())
			rc := s.respClient.Get(s.rt.This().String())
			err := rc.Process(context.Background(), cmd)
			if err != nil {
				return err
			}
			_, err = cmd.Result()
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	cmd := resp.NewGet("mydmap", "mykey").Command(context.Background())
	rc := s.respClient.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)

	value, err := cmd.Bytes()
	require.NoError(t, err)

	var v interface{}
	err = s.serializer.Unmarshal(value, &v)
	require.NoError(t, err)
	require.Equal(t, -100, v.(int))
}

func TestDMap_decrCommandHandler_Single_Request(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := resp.NewDecr("mydmap", "mykey", 100).Command(context.Background())
	rc := s.respClient.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)
	value, err := cmd.Result()

	require.NoError(t, err)
	require.Equal(t, -100, int(value))
}

func TestDMap_exGetPutOperation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var total int64
	var final int64
	start := make(chan struct{})

	getPut := func(i int) error {
		<-start

		value, err := s.serializer.Marshal(i)
		if err != nil {
			return err
		}

		cmd := resp.NewGetPut("mydmap", "mykey", value).Command(context.Background())
		rc := s.respClient.Get(s.rt.This().String())
		err = rc.Process(context.Background(), cmd)
		if err != nil {
			return err
		}
		val, err := cmd.Bytes()
		if err != nil {
			return err
		}

		if len(val) != 0 {
			var oldval interface{}
			err = s.serializer.Unmarshal(val, &oldval)
			if err != nil {
				return err
			}
			if oldval != nil {
				atomic.AddInt64(&total, int64(oldval.(int)))
			}
		}
		return nil
	}

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		num := i
		errGr.Go(func() error {
			return getPut(num)
		})
		final += int64(i)
	}

	close(start)
	require.NoError(t, errGr.Wait())

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	result, err := dm.Get("mykey")
	require.NoError(t, err)

	atomic.AddInt64(&total, int64(result.(int)))
	if atomic.LoadInt64(&total) != final {
		t.Fatalf("Expected %d. Got: %d", final, atomic.LoadInt64(&total))
	}
}
