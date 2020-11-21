// Copyright 2015 The etcd Authors
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

package rafthttp

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/v3/pkg/types"

	"go.uber.org/zap"
)

type failureType struct {
	source string
	action string
}

//peer状态
type peerStatus struct {
	lg     *zap.Logger
	local  types.ID   //本节点的id
	id     types.ID   //对方节点的id
	mu     sync.Mutex // protect variables below 保护下面的成员变量
	active bool       //是否活跃
	since  time.Time  //时间 改变活跃状态的时间，可以是变为活跃的时间或者是变为不活跃的时间
}

//创建一个 peerStatus
func newPeerStatus(lg *zap.Logger, local, id types.ID) *peerStatus {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &peerStatus{lg: lg, local: local, id: id}
}

//将该状态调整为活跃状态，然后记录下当前时间
func (s *peerStatus) activate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		s.lg.Info("peer became active", zap.String("peer-id", s.id.String()))
		s.active = true
		s.since = time.Now()

		activePeers.WithLabelValues(s.local.String(), s.id.String()).Inc()
	}
}

//将该状态调整为不活跃，然后记录下改变的时间
func (s *peerStatus) deactivate(failure failureType, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := fmt.Sprintf("failed to %s %s on %s (%s)", failure.action, s.id, failure.source, reason)
	if s.active {
		s.lg.Warn("peer became inactive (message send to peer failed)", zap.String("peer-id", s.id.String()), zap.Error(errors.New(msg)))
		s.active = false
		s.since = time.Time{}

		activePeers.WithLabelValues(s.local.String(), s.id.String()).Dec()
		disconnectedPeers.WithLabelValues(s.local.String(), s.id.String()).Inc()
		return
	}

	if s.lg != nil {
		s.lg.Debug("peer deactivated again", zap.String("peer-id", s.id.String()), zap.Error(errors.New(msg)))
	}
}

//返回active成员变量，表示是否活跃
func (s *peerStatus) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

//状态改变的时间
func (s *peerStatus) activeSince() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.since
}
