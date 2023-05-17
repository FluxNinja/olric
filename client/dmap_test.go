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
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/testolric"
)

func TestClient_Get(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	key, value := "my-key", "my-value"
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	val, err := c.NewDMap(name).Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", val.(string), value)
	}
}

func TestClient_GetEntry(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	key, value := "my-key", "my-value"
	err = dm.PutEx(key, value, time.Hour)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	entry, err := c.NewDMap(name).GetEntry(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if entry.Value.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", entry.Value.(string), value)
	}
	if entry.Timestamp == 0 {
		t.Fatalf("Timestamp is invalid")
	}
	if entry.TTL == 0 {
		t.Fatalf("TTL is invalid")
	}
}

func TestClient_Put(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	err = c.NewDMap(name).Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	val, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", val.(string), value)
	}

	t.Run("check olric.Entry fields", func(t *testing.T) {
		entry, err := dm.GetEntry(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if entry.Value.(string) != value {
			t.Fatalf("Expected value %s. Got: %s", entry.Value.(string), value)
		}
		if entry.Timestamp == 0 {
			t.Fatalf("Timestamp is invalid")
		}
		if entry.TTL != 0 {
			t.Fatalf("TTL is invalid")
		}
	})
}

func TestClient_PutEx(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	err = c.NewDMap(name).PutEx(key, value, time.Millisecond)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	time.Sleep(time.Millisecond)
	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	v, err := dm.Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
	}
	if v != nil {
		t.Fatalf("Expected nil. Got: %v", v)
	}
}

func TestClient_Delete(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	err = c.NewDMap(name).Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.NewDMap(name).Delete(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = c.NewDMap(name).Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestClient_LockWithTimeout(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "lock.test"
	key := "lock.test.key"
	dm := c.NewDMap(name)
	ctx, err := dm.LockWithTimeout(key, time.Second, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = ctx.Unlock()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_LockWithTimeoutAwaitOtherLock(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "lock.test"
	key := "lock.test.key"
	dm := c.NewDMap(name)
	_, err = dm.LockWithTimeout(key, time.Second, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.LockWithTimeout(key, time.Second, time.Millisecond)
	if err != olric.ErrLockNotAcquired {
		t.Fatalf("Expected olric.ErrLockNotAcquired. Got: %v", err)
	}
}

func TestClient_Lock(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "lock.test"
	key := "lock.test.key"
	dm := c.NewDMap(name)
	ctx, err := dm.Lock(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = ctx.Unlock()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_LockAwaitOtherLock(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "lock.test"
	key := "lock.test.key"
	dm := c.NewDMap(name)
	_, err = dm.Lock(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.Lock(key, time.Millisecond)
	if err != olric.ErrLockNotAcquired {
		t.Fatalf("Expected olric.ErrLockNotAcquired . Got: %v", err)
	}
}

func TestClient_Destroy(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key := "my-key"
	err = c.NewDMap(name).Put(key, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.NewDMap(name).Destroy()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = c.NewDMap(name).Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestClient_Expire(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	key, value := "my-key", "my-value"
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.NewDMap(name).Expire(key, time.Millisecond)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	time.Sleep(time.Millisecond)

	v, err := dm.Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
	}
	if v != nil {
		t.Fatalf("Expected nil. Got: %v", v)
	}
}

func TestClient_PutIf(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	dm := c.NewDMap(name)
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.PutIf(key, "new-value", olric.IfNotFound)
	if err == olric.ErrKeyFound {
		err = nil
	}
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	val, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", val.(string), value)
	}
}

func TestClient_PutIfEx(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	dm := c.NewDMap(name)
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.PutIfEx(key, value, time.Millisecond, olric.IfFound)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	<-time.After(100 * time.Millisecond)
	v, err := dm.Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
	}
	if v != nil {
		t.Fatalf("Expected nil. Got: %v", v)
	}
}
