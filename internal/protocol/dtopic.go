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
	"github.com/go-redis/redis/v8"
)

type Publish struct {
	Topic   string
	Message string
}

func NewPublish(topic, message string) *Publish {
	return &Publish{
		Topic:   topic,
		Message: message,
	}
}

func (p *Publish) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, DTopic.Publish)
	args = append(args, p.Topic)
	args = append(args, p.Message)
	return redis.NewIntCmd(ctx, args...)
}

type Subscribe struct {
	Channels []string
}

func NewSubscribe(channels ...string) *Subscribe {
	return &Subscribe{
		Channels: channels,
	}
}

func (s *Subscribe) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, DTopic.Subscribe)
	for _, channel := range s.Channels {
		args = append(args, channel)
	}
	return redis.NewSliceCmd(ctx, args...)
}

type PSubscribe struct {
	Patterns []string
}

func NewPSubscribe(patterns ...string) *PSubscribe {
	return &PSubscribe{
		Patterns: patterns,
	}
}

func (s *PSubscribe) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, DTopic.Subscribe)
	for _, topic := range s.Patterns {
		args = append(args, topic)
	}
	return redis.NewSliceCmd(ctx, args...)
}

type PubSubChannels struct {
	Pattern string
}

func NewPubSubChannels() *PubSubChannels {
	return &PubSubChannels{}
}

func (ps *PubSubChannels) SetPattern(pattern string) *PubSubChannels {
	ps.Pattern = pattern
	return ps
}

func (ps *PubSubChannels) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, DTopic.PubSubChannels)
	if ps.Pattern != "" {
		args = append(args, ps.Pattern)
	}
	return redis.NewSliceCmd(ctx, args...)
}

type PubSubNumpat struct{}

func NewPubSubNumpat() *PubSubNumpat {
	return &PubSubNumpat{}
}

func (ps *PubSubNumpat) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, DTopic.PubSubChannels)
	return redis.NewIntCmd(ctx, args...)
}

type PubSubNumSub struct {
	Channels []string
}

func NewPubSubNumSub(channels ...string) *PubSubNumSub {
	return &PubSubNumSub{
		Channels: channels,
	}
}

func (ps *PubSubNumSub) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, DTopic.PubSubNumsub)
	for _, channel := range ps.Channels {
		args = append(args, channel)
	}
	return redis.NewSliceCmd(ctx, args...)
}
