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

package protocol

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func stringToCommand(s string) redcon.Command {
	cmd := redcon.Command{
		Raw: []byte(s),
	}

	s = strings.TrimSuffix(s, ": []")
	s = strings.TrimSuffix(s, ": 0")
	s = strings.TrimSuffix(s, ":")
	s = strings.TrimSuffix(s, ": ")
	parsed := strings.Split(s, " ")
	for _, arg := range parsed {
		cmd.Args = append(cmd.Args, []byte(arg))
	}
	return cmd
}

func TestProtocol_ParsePutCommand_EX(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	putCmd.SetEX((10 * time.Second).Seconds())

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.Equal(t, float64(10), parsed.EX)
}

func TestProtocol_ParsePutCommand_PX(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	putCmd.SetPX((100 * time.Millisecond).Milliseconds())

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.Equal(t, int64(100), parsed.PX)
}

func TestProtocol_ParsePutCommand_NX(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	putCmd.SetNX()

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.True(t, parsed.NX)
	require.False(t, parsed.XX)
}

func TestProtocol_ParsePutCommand_XX(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	putCmd.SetXX()

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.True(t, parsed.XX)
	require.False(t, parsed.NX)
}

func TestProtocol_ParsePutCommand_EXAT(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	exat := float64(time.Now().Unix()) + 10
	putCmd.SetEXAT(exat)

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.Equal(t, exat, parsed.EXAT)
}

func TestProtocol_ParsePutCommand_PXAT(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	pxat := (time.Now().UnixNano() / 1000000) + 10
	putCmd.SetPXAT(pxat)

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.Equal(t, pxat, parsed.PXAT)
}

func TestProtocol_ParseScanCommand(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0)

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "", parsed.Match)
	require.Equal(t, 10, parsed.Count)
	require.False(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_Replica(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).SetReplica()

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "", parsed.Match)
	require.Equal(t, 10, parsed.Count)
	require.True(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_Match(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).SetMatch("^even")

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(1), parsed.PartID)
	require.Equal(t, "^even", parsed.Match)
	require.Equal(t, 10, parsed.Count)
	require.False(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_PartID(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).SetCount(200)

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(1), parsed.PartID)
	require.Equal(t, "", parsed.Match)
	require.Equal(t, 200, parsed.Count)
	require.False(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_Match_Count(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).SetCount(100).SetMatch("^even")

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(1), parsed.PartID)
	require.Equal(t, "^even", parsed.Match)
	require.Equal(t, 100, parsed.Count)
	require.False(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_Match_Count_Replica(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).
		SetCount(100).
		SetMatch("^even").
		SetReplica()

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(1), parsed.PartID)
	require.Equal(t, "^even", parsed.Match)
	require.Equal(t, 100, parsed.Count)
	require.True(t, scanCmd.Replica)
}
