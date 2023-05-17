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

package client

import (
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testolric"
)

func TestPipeline_Put(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	p := c.NewPipeline()

	// Put some keys
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Decode responses
	for _, res := range responses {
		if res.response.OpCode() != protocol.OpPut {
			t.Fatalf("Expected Op: %v. Got: %v", protocol.OpPut, res.response.OpCode())
		}
		if res.response.Status() != protocol.StatusOK {
			t.Fatalf("Expected StatusCode: %v. Got: %v", protocol.StatusOK, res.response.Status())
		}
	}
}

func TestPipeline_Get(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	p := c.NewPipeline()

	dmap := "mydmap"
	key := "key-" + strconv.Itoa(1)

	// Put the key
	err = p.Put(dmap, key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Get the key
	err = p.Get(dmap, key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Flush commands
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for _, res := range responses {
		if res.Operation() == "Put" {
			err := res.Put()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
		if res.Operation() == "Get" {
			val, err := res.Get()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if val.(int) != 1 {
				t.Fatalf("Expected 1. Got: %v", val)
			}
		}
	}
}

func TestPipeline_PutEx(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	p := c.NewPipeline()

	// Put some keys with ttl
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.PutEx(dmap, key, i, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Decode responses
	for _, res := range responses {
		if res.response.OpCode() != protocol.OpPutEx {
			t.Fatalf("Expected Op: %v. Got: %v", protocol.OpPutEx, res.response.OpCode())
		}
		if res.response.Status() != protocol.StatusOK {
			t.Fatalf("Expected StatusCode: %v. Got: %v", protocol.StatusOK, res.response.Status())
		}
	}
}

func TestPipeline_Delete(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	p := c.NewPipeline()

	// Put some keys
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Delete the keys
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Delete(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Decode responses
	for _, res := range responses {
		if res.Operation() == "Put" {
			err = res.Put()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
		if res.Operation() == "Delete" {
			err = res.Delete()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
	}

	// Try to get the keys
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Get(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush commands
	responses, err = p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for _, res := range responses {
		_, err := res.Get()
		if err != olric.ErrKeyNotFound {
			t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestPipeline_Destroy(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	p := c.NewPipeline()

	dmap := "mydmap"
	key := "key"
	// Put the key
	err = p.Put(dmap, key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Call GetPut to update the key
	err = p.Destroy(dmap)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Get the current value with Get
	err = p.Get(dmap, key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Flush commands
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for _, res := range responses {
		if res.Operation() == "Put" {
			err := res.Put()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
		if res.Operation() == "Destroy" {
			err := res.Destroy()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}

		if res.Operation() == "Get" {
			_, err := res.Get()
			if err != olric.ErrKeyNotFound {
				t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
			}
		}
	}
}

func TestPipeline_Expire(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	p := c.NewPipeline()

	// Put some keys with ttl
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Expire(dmap, key, time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Decode responses
	for _, res := range responses {
		if res.response.OpCode() != protocol.OpPut && res.response.OpCode() != protocol.OpExpire {
			t.Fatalf("Expected Op: %v or %v. Got: %v",
				protocol.OpPut, protocol.OpExpire, res.response.OpCode())
		}
		if res.response.Status() != protocol.StatusOK {
			t.Fatalf("Expected StatusCode: %v. Got: %v", protocol.StatusOK, res.response.Status())
		}
	}

	<-time.After(200 * time.Millisecond)
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Get(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	responses, err = p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Decode responses
	for _, res := range responses {
		if res.response.Status() != protocol.StatusErrKeyNotFound {
			t.Fatalf("Expected StatusCode: %v. Got: %v", protocol.StatusErrKeyNotFound, res.response.Status())
		}
	}
}

func TestPipeline_PutIf(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	p := c.NewPipeline()

	// Put some keys
	dmap := "mydmap"
	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.PutIf(dmap, key, (i*100)+1, olric.IfNotFound)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Get(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Decode responses
	for _, res := range responses {
		if res.response.OpCode() == protocol.OpPutIf {
			if res.response.Status() != protocol.StatusErrKeyFound {
				t.Fatalf("Expected StatusCode: %v. Got: %v", protocol.StatusErrKeyFound, res.response.Status())
			}
		}

		if res.response.OpCode() == protocol.OpGet {
			val, err := res.Get()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if val.(int) > 100 {
				t.Fatalf("value changed: %v", val)
			}
		}
	}
}

func TestPipeline_PutIfEx(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	p := c.NewPipeline()

	// Put some keys
	dmap := "mydmap"
	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.PutIfEx(dmap, key, (i*100)+1, time.Millisecond, olric.IfFound)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	_, err = p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	<-time.After(200 * time.Millisecond)
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Get(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Decode responses
	for _, res := range responses {
		if res.response.Status() != protocol.StatusErrKeyNotFound {
			t.Fatalf("Expected StatusCode: %v. Got: %v", protocol.StatusErrKeyNotFound, res.response.Status())
		}
	}
}
