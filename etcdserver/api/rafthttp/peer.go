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
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/v3/pkg/types"
	"go.etcd.io/etcd/v3/raft"
	"go.etcd.io/etcd/v3/raft/raftpb"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	ConnReadTimeout  = 5 * time.Second
	ConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
	maxPendingProposals = 4096

	streamAppV2 = "streamMsgAppV2"
	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
	sendSnap    = "sendMsgSnap"
)

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	// send 发送message到remote peer。 该函数是非阻塞的，并且不保证message被remote接收。
	// 当发送message失败后，它将状态报告给底层的raft
	send(m raftpb.Message)

	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	sendSnap(m snap.Message)

	// update updates the urls of remote peer.
	// 更新remote peer的urls
	update(urls types.URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	//attachOutgoingConn将传出连接附加到对等方以供流使用。 呼叫之后，传出连接的所有权移交给对等方。 当不再使用连接时，对等方将关闭该连接。
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	// 返回当与peer的连接变活跃后的time
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	stop()
}

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// peer 是remote raft node的代表。 本地raft node通过peer与remote peer发送数据。
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// 每个 peer 有两种底层机制来发送 message：stream和pipeline
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// stream是接收器初始化的长轮询连接，该连接始终打开以传输消息。
//除了一般流之外，对等方还具有用于发送msgApp的优化流，因为msgApp占了所有消息的很大一部分。
//仅raft领导者使用优化的流将msgApp发送到远程follower节点。
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
// 一个 pipeline 是一系列 http clients，他们发送http request 到 remote peer。
// 仅在尚未建立流时使用。
type peer struct {
	lg *zap.Logger

	localID types.ID
	// id of the remote raft peer node
	id types.ID

	r Raft

	status *peerStatus

	picker *urlPicker

	msgAppV2Writer *streamWriter
	writer         *streamWriter
	pipeline       *pipeline
	snapSender     *snapshotSender // snapshot sender to send v3 snapshot messages
	msgAppV2Reader *streamReader
	msgAppReader   *streamReader

	recvc chan raftpb.Message
	propc chan raftpb.Message

	mu     sync.Mutex
	paused bool

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func startPeer(t *Transport, urls types.URLs, peerID types.ID, fs *stats.FollowerStats) *peer {
	if t.Logger != nil {
		t.Logger.Info("starting remote peer", zap.String("remote-peer-id", peerID.String()))
	}
	defer func() {
		if t.Logger != nil {
			t.Logger.Info("started remote peer", zap.String("remote-peer-id", peerID.String()))
		}
	}()

	status := newPeerStatus(t.Logger, t.ID, peerID)
	picker := newURLPicker(urls)
	errorc := t.ErrorC
	r := t.Raft
	pipeline := &pipeline{
		peerID:        peerID,
		tr:            t,
		picker:        picker,
		status:        status,
		followerStats: fs,
		raft:          r,
		errorc:        errorc,
	}
	pipeline.start()

	p := &peer{
		lg:             t.Logger,
		localID:        t.ID,
		id:             peerID,
		r:              r,
		status:         status,
		picker:         picker,
		msgAppV2Writer: startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		writer:         startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		pipeline:       pipeline,
		snapSender:     newSnapshotSender(t, picker, peerID, status),
		recvc:          make(chan raftpb.Message, recvBufSize),
		propc:          make(chan raftpb.Message, maxPendingProposals),
		stopc:          make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go func() {
		for {
			select {
			case mm := <-p.recvc: // 接收消息的协程
				if err := r.Process(ctx, mm); err != nil {
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.
	// Thus propc must be put into a separate routine with recvc to avoid blocking
	// processing other raft messages.
	go func() {
		for {
			select {
			case mm := <-p.propc:
				if err := r.Process(ctx, mm); err != nil {
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	p.msgAppV2Reader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMsgAppV2,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}
	p.msgAppReader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMessage,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}

	p.msgAppV2Reader.start()
	p.msgAppReader.start()

	return p
}

func (p *peer) send(m raftpb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused {
		return
	}

	writec, name := p.pick(m)
	select {
	case writec <- m:
	default:
		p.r.ReportUnreachable(m.To)
		if isMsgSnap(m) {
			p.r.ReportSnapshot(m.To, raft.SnapshotFailure)
		}
		if p.status.isActive() {
			if p.lg != nil {
				p.lg.Warn(
					"dropped internal Raft message since sending buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", p.localID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", p.id.String()),
					zap.String("remote-peer-name", name),
					zap.Bool("remote-peer-active", p.status.isActive()),
				)
			}
		} else {
			if p.lg != nil {
				p.lg.Warn(
					"dropped internal Raft message since sending buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", p.localID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", p.id.String()),
					zap.String("remote-peer-name", name),
					zap.Bool("remote-peer-active", p.status.isActive()),
				)
			}
		}
		sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
	}
}

func (p *peer) sendSnap(m snap.Message) {
	go p.snapSender.send(m)
}

func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}

func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMsgAppV2:
		ok = p.msgAppV2Writer.attach(conn)
	case streamTypeMessage:
		ok = p.writer.attach(conn)
	default:
		if p.lg != nil {
			p.lg.Panic("unknown stream type", zap.String("type", conn.t.String()))
		}
	}
	if !ok {
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
	p.msgAppV2Reader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
	p.msgAppV2Reader.resume()
}

func (p *peer) stop() {
	if p.lg != nil {
		p.lg.Info("stopping remote peer", zap.String("remote-peer-id", p.id.String()))
	}

	defer func() {
		if p.lg != nil {
			p.lg.Info("stopped remote peer", zap.String("remote-peer-id", p.id.String()))
		}
	}()

	close(p.stopc)
	p.cancel()
	p.msgAppV2Writer.stop()
	p.writer.stop()
	p.pipeline.stop()
	p.snapSender.stop()
	p.msgAppV2Reader.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
func (p *peer) pick(m raftpb.Message) (writec chan<- raftpb.Message, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.
	if isMsgSnap(m) {
		return p.pipeline.msgc, pipelineMsg
	} else if writec, ok = p.msgAppV2Writer.writec(); ok && isMsgApp(m) {
		return writec, streamAppV2
	} else if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg
}

func isMsgApp(m raftpb.Message) bool { return m.Type == raftpb.MsgApp }

func isMsgSnap(m raftpb.Message) bool { return m.Type == raftpb.MsgSnap }
