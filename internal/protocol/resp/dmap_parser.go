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

package resp

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/buraksezer/olric/internal/util"
	"github.com/tidwall/redcon"
)

var ErrInvalidArgument = errors.New("invalid argument")

func ParsePutCommand(cmd redcon.Command) (*Put, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	p := NewPut(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		cmd.Args[3],                     // Value
	)

	args := cmd.Args[4:]
	for len(args) > 0 {
		switch arg := strings.ToUpper(util.BytesToString(args[0])); arg {
		case "NX":
			p.SetNX()
			args = args[1:]
			continue
		case "XX":
			p.SetXX()
			args = args[1:]
			continue
		case "PX":
			px, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPX(px)
			args = args[2:]
			continue
		case "EX":
			ex, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEX(ex)
			args = args[2:]
			continue
		case "EXAT":
			exat, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEXAT(exat)
			args = args[2:]
			continue
		case "PXAT":
			pxat, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPXAT(pxat)
			args = args[2:]
			continue
		default:
			return nil, errors.New("syntax error")
		}
	}

	return p, nil
}

func ParseGetEntryCommand(cmd redcon.Command) (*GetEntry, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	g := NewGetEntry(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
	)

	if len(cmd.Args) == 4 {
		arg := util.BytesToString(cmd.Args[3])
		if arg == "RC" {
			g.SetReplica()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return g, nil
}

func ParseGetCommand(cmd redcon.Command) (*Get, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewGet(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
	), nil
}

func ParsePutReplicaCommand(cmd redcon.Command) (*PutReplica, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewPutReplica(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
		cmd.Args[3],
	), nil
}

func ParseDelCommand(cmd redcon.Command) (*Del, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewDel(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
	), nil
}

func ParseDelEntryCommand(cmd redcon.Command) (*DelEntry, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	d := NewDelEntry(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
	)

	if len(cmd.Args) == 4 {
		arg := util.BytesToString(cmd.Args[3])
		if arg == "RC" {
			d.SetReplica()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return d, nil
}

func ParseExpireCommand(cmd redcon.Command) (*Expire, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	rawSeconds := util.BytesToString(cmd.Args[3])
	seconds, err := strconv.ParseFloat(rawSeconds, 64)
	if err != nil {
		return nil, err
	}
	e := NewExpire(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
		seconds,
	)

	if len(cmd.Args) == 5 {
		arg := util.BytesToString(cmd.Args[4])
		if arg == "RC" {
			e.SetReplica()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return e, nil
}

func ParseDestroyCommand(cmd redcon.Command) (*Destroy, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	d := NewDestroy(
		util.BytesToString(cmd.Args[1]),
	)

	if len(cmd.Args) == 3 {
		arg := util.BytesToString(cmd.Args[2])
		if arg == "LC" {
			d.SetLocal()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return d, nil
}

func ParseQueryCommand(cmd redcon.Command) (*Query, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	partID, err := strconv.ParseUint(util.BytesToString(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}

	q := NewQuery(
		util.BytesToString(cmd.Args[1]),
		partID,
		cmd.Args[3],
	)

	if len(cmd.Args) == 5 {
		arg := util.BytesToString(cmd.Args[4])
		if arg == "LC" {
			q.SetLocal()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return q, nil
}

func ParseIncrCommand(cmd redcon.Command) (*Incr, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	delta, err := strconv.Atoi(util.BytesToString(cmd.Args[3]))
	if err != nil {
		return nil, err
	}

	return NewIncr(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
		delta,
	), nil
}

func ParseDecrCommand(cmd redcon.Command) (*Decr, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	delta, err := strconv.Atoi(util.BytesToString(cmd.Args[3]))
	if err != nil {
		return nil, err
	}

	return NewDecr(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
		delta,
	), nil
}

func ParseGetPutCommand(cmd redcon.Command) (*GetPut, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewGetPut(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		cmd.Args[3],                     // Value
	), nil
}
