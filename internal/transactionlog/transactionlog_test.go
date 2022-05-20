// Copyright 2018-2022 Burak Sezer
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
package transactionlog

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/transactionlog/config"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func newRedisClient(t *testing.T, c *config.Config) *redis.Client {
	rc := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(c.OlricTransactionLog.BindAddr, strconv.Itoa(c.OlricTransactionLog.BindPort)),
	})
	t.Cleanup(func() {
		require.NoError(t, rc.Close())
	})
	return rc
}

func makeResolverConfig(t *testing.T) *config.Config {
	var (
		_, b, _, _ = runtime.Caller(0)
		basepath   = filepath.Dir(b)
	)

	filename := path.Join(basepath, "config/fixtures/olric-transaction-log.yaml")
	c, err := config.New(filename)
	require.NoError(t, err)

	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.OlricTransactionLog.BindPort = port

	tmpdir, err := ioutil.TempDir("", "olric-transaction-log-test")
	require.NoError(t, err)
	c.OlricTransactionLog.DataDir = tmpdir

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(c.OlricTransactionLog.DataDir))
	})

	return c
}

func TestTransactionLog_Start_Shutdown(t *testing.T) {
	c := makeResolverConfig(t)

	lg := log.New(os.Stderr, "", 0)
	sq, err := New(c, lg)
	require.NoError(t, err)

	errCh := make(chan error)
	go func() {
		errCh <- sq.Start()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rc := newRedisClient(t, c)
	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		cmd := rc.Ping(ctx)
		return cmd.Err()
	})
	require.NoError(t, err)

	require.NoError(t, sq.Shutdown(ctx))
	require.NoError(t, <-errCh)
}
