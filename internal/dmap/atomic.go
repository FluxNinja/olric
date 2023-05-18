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
	"bytes"
	"encoding/gob"
)

func (dm *DMap) Incr(key string, delta float64) (float64, error) {
	// marshal delta as gob
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(delta)
	if err != nil {
		return 0, err
	}
	deltaBytes := buf.Bytes()

	resultBytes, err := dm.Function(key, AddFunction, deltaBytes)
	if err != nil {
		return 0, err
	}

	// unmarshal result as float64
	var result float64
	buf = bytes.NewBuffer(resultBytes)
	err = gob.NewDecoder(buf).Decode(&result)
	if err != nil {
		return 0, err
	}
	return result, nil
}

func (dm *DMap) Decr(key string, delta float64) (float64, error) {
	return dm.Incr(key, -delta)
}
