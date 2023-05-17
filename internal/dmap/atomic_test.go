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
	"sync"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
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
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go incr(dm)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if res.(int) != 100 {
		t.Fatalf("Expected 100. Got: %v", res)
	}
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
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go decr(dm)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if res.(int) != -100 {
		t.Fatalf("Expected 100. Got: %v", res)
	}
}

func TestDMap_Atomic_Honor_Current_TTL(t *testing.T) {
	// See https://github.com/buraksezer/olric/pull/172
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "decr"

	dm, err := s.NewDMap("atomic_test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dm.PutEx(key, 10, time.Hour)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.Decr(key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	entry, err := dm.GetEntry(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if entry.TTL == 0 {
		t.Fatal("entry.TTL cannot be zero")
	}
}
