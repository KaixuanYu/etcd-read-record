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

package v3rpc

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"time"

	"go.etcd.io/etcd/v3/auth"
	"go.etcd.io/etcd/v3/etcdserver"
	"go.etcd.io/etcd/v3/etcdserver/api/v3rpc/rpctypes"
	pb "go.etcd.io/etcd/v3/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/v3/mvcc"
	"go.etcd.io/etcd/v3/mvcc/mvccpb"

	"go.uber.org/zap"
)

type watchServer struct {
	lg *zap.Logger

	clusterID int64
	memberID  int64

	maxRequestBytes int

	sg        etcdserver.RaftStatusGetter
	watchable mvcc.WatchableKV // 这个其实就是 mvcc.watchableStore
	ag        AuthGetter
}

// NewWatchServer returns a new watch server.
func NewWatchServer(s *etcdserver.EtcdServer) pb.WatchServer {
	srv := &watchServer{
		lg: s.Cfg.Logger,

		clusterID: int64(s.Cluster().ID()),
		memberID:  int64(s.ID()),

		maxRequestBytes: int(s.Cfg.MaxRequestBytes + grpcOverheadBytes),

		sg:        s,
		watchable: s.Watchable(),
		ag:        s,
	}
	if srv.lg == nil {
		srv.lg = zap.NewNop()
	}
	return srv
}

var (
	// External test can read this with GetProgressReportInterval()
	// and change this to a small value to finish fast with
	// SetProgressReportInterval().
	//外部测试可以使用GetProgressReportInterval（）读取此参数，并将其更改为一个小值，以使用SetProgressReportInterval（）快速完成。
	progressReportInterval   = 10 * time.Minute //进展上报时间间隔？
	progressReportIntervalMu sync.RWMutex       // 进展上报时间间隔锁
)

// GetProgressReportInterval returns the current progress report interval (for testing).
// GetProgressReportInterval 返回当前的 进展上报时间间隔 （为了测试）
func GetProgressReportInterval() time.Duration {
	progressReportIntervalMu.RLock()
	interval := progressReportInterval
	progressReportIntervalMu.RUnlock()

	// add rand(1/10*progressReportInterval) as jitter so that etcdserver will not
	// send progress notifications to watchers around the same time even when watchers
	// are created around the same time (which is common when a client restarts itself).
	// 添加rand（1/10*progressReportInterval）作为抖动，
	// 这样etcdserver就不会在同一时间向观察者发送进度通知，
	// 即使观察者是在大约同一时间创建的（这在客户端重新启动时很常见）
	jitter := time.Duration(rand.Int63n(int64(interval) / 10))

	return interval + jitter
}

// SetProgressReportInterval updates the current progress report interval (for testing).
func SetProgressReportInterval(newTimeout time.Duration) {
	progressReportIntervalMu.Lock()
	progressReportInterval = newTimeout
	progressReportIntervalMu.Unlock()
}

// We send ctrl response inside the read loop. We do not want
// send to block read, but we still want ctrl response we sent to
// be serialized. Thus we use a buffered chan to solve the problem.
// A small buffer should be OK for most cases, since we expect the
// ctrl requests are infrequent.
const ctrlStreamBufLen = 16

// serverWatchStream is an etcd server side stream. It receives requests
// from client side gRPC stream. It receives watch events from mvcc.WatchStream,
// and creates responses that forwarded to gRPC stream.
// It also forwards control message like watch created and canceled.
type serverWatchStream struct {
	lg *zap.Logger

	clusterID int64
	memberID  int64

	maxRequestBytes int

	sg        etcdserver.RaftStatusGetter
	watchable mvcc.WatchableKV
	ag        AuthGetter

	gRPCStream  pb.Watch_WatchServer // 这是 grpc 的流服务。有send和recv方法。
	watchStream mvcc.WatchStream
	ctrlStream  chan *pb.WatchResponse

	// mu protects progress, prevKV, fragment
	mu sync.RWMutex
	// tracks the watchID that stream might need to send progress to
	// 跟踪watchID,流可能需要send 进展到该watchID
	// TODO: combine progress and prevKV into a single struct?
	// 将progress 和prevKV合并到一个结构体？
	progress map[mvcc.WatchID]bool
	// record watch IDs that need return previous key-value pair
	// 记录需要返回先前键值对的watch ID
	prevKV map[mvcc.WatchID]bool
	// records fragmented watch IDs
	// 记录零碎的watch ID
	fragment map[mvcc.WatchID]bool

	// closec indicates the stream is closed.
	closec chan struct{}

	// wg waits for the send loop to complete
	wg sync.WaitGroup
}

func (ws *watchServer) Watch(stream pb.Watch_WatchServer) (err error) {
	sws := serverWatchStream{
		lg: ws.lg,

		clusterID: ws.clusterID,
		memberID:  ws.memberID,

		maxRequestBytes: ws.maxRequestBytes,

		sg:        ws.sg,
		watchable: ws.watchable,
		ag:        ws.ag,

		gRPCStream:  stream,
		watchStream: ws.watchable.NewWatchStream(),
		// chan for sending control response like watcher created and canceled.
		// chan用来发送control response ，像watcher 创建和取消
		ctrlStream: make(chan *pb.WatchResponse, ctrlStreamBufLen),

		progress: make(map[mvcc.WatchID]bool),
		prevKV:   make(map[mvcc.WatchID]bool),
		fragment: make(map[mvcc.WatchID]bool),

		closec: make(chan struct{}),
	}

	sws.wg.Add(1)
	go func() {
		sws.sendLoop()
		sws.wg.Done()
	}()

	errc := make(chan error, 1)
	// Ideally recvLoop would also use sws.wg to signal its completion
	// but when stream.Context().Done() is closed, the stream's recv
	// may continue to block since it uses a different context, leading to
	// deadlock when calling sws.close().
	//理想情况下recvLoop也将使用sws.wg来指示其完成，但是当stream.Context（）。Done（）关闭时，流的recv可能会继续阻塞，因为它使用了不同的上下文，从而在调用sws.close时导致死锁 （）。
	go func() {
		if rerr := sws.recvLoop(); rerr != nil {
			if isClientCtxErr(stream.Context().Err(), rerr) {
				sws.lg.Debug("failed to receive watch request from gRPC stream", zap.Error(rerr))
			} else {
				sws.lg.Warn("failed to receive watch request from gRPC stream", zap.Error(rerr))
				streamFailures.WithLabelValues("receive", "watch").Inc()
			}
			errc <- rerr
		}
	}()

	select {
	case err = <-errc:
		close(sws.ctrlStream)

	case <-stream.Context().Done():
		err = stream.Context().Err()
		// the only server-side cancellation is noleader for now.
		if err == context.Canceled {
			err = rpctypes.ErrGRPCNoLeader
		}
	}

	sws.close()
	return err
}

//是否允许watch
func (sws *serverWatchStream) isWatchPermitted(wcr *pb.WatchCreateRequest) bool {
	authInfo, err := sws.ag.AuthInfoFromCtx(sws.gRPCStream.Context())
	if err != nil {
		return false
	}
	if authInfo == nil {
		// if auth is enabled, IsRangePermitted() can cause an error
		authInfo = &auth.AuthInfo{}
	}
	return sws.ag.AuthStore().IsRangePermitted(authInfo, wcr.Key, wcr.RangeEnd) == nil
}

//处理流接收请求。有create请求，cancel请求和progress请求
func (sws *serverWatchStream) recvLoop() error {
	for {
		req, err := sws.gRPCStream.Recv() //从client接收一个请求request ，看着有3种请求，create ， cancel 和 progress
		if err == io.EOF {                //如果已经读完流消息
			return nil
		}
		if err != nil { //读出现错误
			return err
		}

		switch uv := req.RequestUnion.(type) {
		case *pb.WatchRequest_CreateRequest: //如果是创建请求
			if uv.CreateRequest == nil {
				break
			}

			creq := uv.CreateRequest
			if len(creq.Key) == 0 {
				// \x00 is the smallest key
				creq.Key = []byte{0}
			}
			if len(creq.RangeEnd) == 0 {
				// force nil since watchstream.Watch distinguishes
				// between nil and []byte{} for single key / >=
				// 强制 nil ，因为watchstream.Watch区分nil和[]byte{}
				creq.RangeEnd = nil
			}
			if len(creq.RangeEnd) == 1 && creq.RangeEnd[0] == 0 {
				// support  >= key queries
				// 允许 >= key 查询
				creq.RangeEnd = []byte{}
			}

			if !sws.isWatchPermitted(creq) { //如果该watch不允许创建
				wr := &pb.WatchResponse{
					Header:       sws.newResponseHeader(sws.watchStream.Rev()),
					WatchId:      creq.WatchId,
					Canceled:     true,
					Created:      true,
					CancelReason: rpctypes.ErrGRPCPermissionDenied.Error(),
				}

				select {
				case sws.ctrlStream <- wr: //发送一个watch不允许创建的Response
					continue
				case <-sws.closec:
					return nil
				}
			}

			filters := FiltersFromRequest(creq) //添加watch过滤事件，比如想要某个key不监听删除操作，或者不监听put操作（就这俩操作会被监听）

			wsrev := sws.watchStream.Rev()
			rev := creq.StartRevision
			if rev == 0 {
				rev = wsrev + 1
			}
			// 创建了一个watch，返回了该watch的id
			id, err := sws.watchStream.Watch(mvcc.WatchID(creq.WatchId), creq.Key, creq.RangeEnd, rev, filters...)
			if err == nil {
				sws.mu.Lock()
				if creq.ProgressNotify { //进度通知
					sws.progress[id] = true
				}
				if creq.PrevKv {
					sws.prevKV[id] = true
				}
				if creq.Fragment {
					sws.fragment[id] = true
				}
				sws.mu.Unlock()
			}
			wr := &pb.WatchResponse{
				Header:   sws.newResponseHeader(wsrev),
				WatchId:  int64(id),
				Created:  true,
				Canceled: err != nil,
			}
			if err != nil {
				wr.CancelReason = err.Error()
			}
			select {
			case sws.ctrlStream <- wr: //发送正常的以创建响应
			case <-sws.closec:
				return nil
			}

		case *pb.WatchRequest_CancelRequest: //如果是取消请求
			if uv.CancelRequest != nil {
				id := uv.CancelRequest.WatchId
				err := sws.watchStream.Cancel(mvcc.WatchID(id)) //调用了mvcc 的watch的Cancel
				if err == nil {
					//发送已经删除的响应
					sws.ctrlStream <- &pb.WatchResponse{
						Header:   sws.newResponseHeader(sws.watchStream.Rev()),
						WatchId:  id,
						Canceled: true,
					}
					sws.mu.Lock()
					delete(sws.progress, mvcc.WatchID(id))
					delete(sws.prevKV, mvcc.WatchID(id))
					delete(sws.fragment, mvcc.WatchID(id))
					sws.mu.Unlock()
				}
			}
		case *pb.WatchRequest_ProgressRequest: //如果是 progressRequest
			if uv.ProgressRequest != nil {
				sws.ctrlStream <- &pb.WatchResponse{
					Header:  sws.newResponseHeader(sws.watchStream.Rev()),
					WatchId: -1, // response is not associated with any WatchId and will be broadcast to all watch channels
					//响应与任何WatchId不相关，并将广播到所有观看频道
				}
			}
		default:
			// we probably should not shutdown the entire stream when
			// receive an valid command.
			// so just do nothing instead.
			//当收到有效命令时，我们可能不应该关闭整个流。 所以什么也不要做。
			continue
		}
	}
}

func (sws *serverWatchStream) sendLoop() {
	// watch ids that are currently active
	// 当前活跃的watch ids
	// ids 记录了当前有client与之保持连接的watchID。如果该watchID有client用，如果发生key变动event，就会直接发送给client
	// 如果该watchID没有client用，那么如果发生key变动event，就先存在pending中缓存起来，如果之后有client用watchID监听，那么先将buf中所有的event发送给client
	// 然后清空pending[watchID]。
	ids := make(map[mvcc.WatchID]struct{})
	// watch responses pending on a watch id creation message
	// 等待watch id创建消息的watch responses
	pending := make(map[mvcc.WatchID][]*pb.WatchResponse)

	interval := GetProgressReportInterval()
	progressTicker := time.NewTicker(interval) //10分钟左右的ticker

	defer func() {
		progressTicker.Stop()
		// drain the chan to clean up pending events
		for ws := range sws.watchStream.Chan() {
			mvcc.ReportEventReceived(len(ws.Events))
		}
		for _, wrs := range pending {
			for _, ws := range wrs {
				mvcc.ReportEventReceived(len(ws.Events))
			}
		}
	}()

	for {
		select {
		//这里处理发生改变的key的事件。
		case wresp, ok := <-sws.watchStream.Chan(): // buf 是 128的chan
			if !ok {
				return
			}

			// TODO: evs is []mvccpb.Event type
			// either return []*mvccpb.Event from the mvcc package
			// or define protocol buffer with []mvccpb.Event.
			evs := wresp.Events
			events := make([]*mvccpb.Event, len(evs)) //装 prevKV 的
			sws.mu.RLock()
			needPrevKV := sws.prevKV[wresp.WatchID]
			sws.mu.RUnlock()
			for i := range evs {
				events[i] = &evs[i]
				if needPrevKV {
					opt := mvcc.RangeOptions{Rev: evs[i].Kv.ModRevision - 1}
					r, err := sws.watchable.Range(evs[i].Kv.Key, nil, opt)
					if err == nil && len(r.KVs) != 0 {
						events[i].PrevKv = &(r.KVs[0])
					}
				}
			}

			canceled := wresp.CompactRevision != 0
			wr := &pb.WatchResponse{
				Header:          sws.newResponseHeader(wresp.Revision),
				WatchId:         int64(wresp.WatchID),
				Events:          events,
				CompactRevision: wresp.CompactRevision,
				Canceled:        canceled,
			}

			if _, okID := ids[wresp.WatchID]; !okID {
				// buffer if id not yet announced 如果尚未公布ID，请缓冲
				wrs := append(pending[wresp.WatchID], wr)
				pending[wresp.WatchID] = wrs
				continue
			}

			mvcc.ReportEventReceived(len(evs))

			sws.mu.RLock()
			fragmented, ok := sws.fragment[wresp.WatchID]
			sws.mu.RUnlock()

			var serr error
			if !fragmented && !ok {
				serr = sws.gRPCStream.Send(wr)
			} else {
				// 分段发送
				serr = sendFragments(wr, sws.maxRequestBytes, sws.gRPCStream.Send)
			}

			if serr != nil {
				if isClientCtxErr(sws.gRPCStream.Context().Err(), serr) {
					sws.lg.Debug("failed to send watch response to gRPC stream", zap.Error(serr))
				} else {
					sws.lg.Warn("failed to send watch response to gRPC stream", zap.Error(serr))
					streamFailures.WithLabelValues("send", "watch").Inc()
				}
				return
			}

			sws.mu.Lock()
			if len(evs) > 0 && sws.progress[wresp.WatchID] {
				// elide next progress update if sent a key update
				// 如果发送了关键更新，则忽略下一个进度更新
				sws.progress[wresp.WatchID] = false
			}
			sws.mu.Unlock()
		//这里处理创建和取消watch的操作
		case c, ok := <-sws.ctrlStream:
			if !ok {
				return
			}

			//将响应发送给client
			if err := sws.gRPCStream.Send(c); err != nil {
				if isClientCtxErr(sws.gRPCStream.Context().Err(), err) {
					sws.lg.Debug("failed to send watch control response to gRPC stream", zap.Error(err))
				} else {
					sws.lg.Warn("failed to send watch control response to gRPC stream", zap.Error(err))
					streamFailures.WithLabelValues("send", "watch").Inc()
				}
				return
			}

			// track id creation
			wid := mvcc.WatchID(c.WatchId)
			if c.Canceled { //如果已经删除
				delete(ids, wid)
				continue
			}
			if c.Created {
				// flush buffered events
				ids[wid] = struct{}{}
				for _, v := range pending[wid] { //将缓冲区的都发送给client
					mvcc.ReportEventReceived(len(v.Events)) //普罗米修斯监控
					if err := sws.gRPCStream.Send(v); err != nil {
						if isClientCtxErr(sws.gRPCStream.Context().Err(), err) {
							sws.lg.Debug("failed to send pending watch response to gRPC stream", zap.Error(err))
						} else {
							sws.lg.Warn("failed to send pending watch response to gRPC stream", zap.Error(err))
							streamFailures.WithLabelValues("send", "watch").Inc()
						}
						return
					}
				}
				delete(pending, wid)
			}

		case <-progressTicker.C:
			sws.mu.Lock()
			for id, ok := range sws.progress {
				if ok {
					sws.watchStream.RequestProgress(id)
				}
				sws.progress[id] = true
			}
			sws.mu.Unlock()

		case <-sws.closec:
			return
		}
	}
}

func sendFragments(
	wr *pb.WatchResponse,
	maxRequestBytes int,
	sendFunc func(*pb.WatchResponse) error) error {
	// no need to fragment if total request size is smaller
	// than max request limit or response contains only one event
	if wr.Size() < maxRequestBytes || len(wr.Events) < 2 {
		return sendFunc(wr)
	}

	ow := *wr
	ow.Events = make([]*mvccpb.Event, 0)
	ow.Fragment = true

	var idx int
	for {
		cur := ow
		for _, ev := range wr.Events[idx:] {
			cur.Events = append(cur.Events, ev)
			if len(cur.Events) > 1 && cur.Size() >= maxRequestBytes {
				cur.Events = cur.Events[:len(cur.Events)-1]
				break
			}
			idx++
		}
		if idx == len(wr.Events) {
			// last response has no more fragment
			cur.Fragment = false
		}
		if err := sendFunc(&cur); err != nil {
			return err
		}
		if !cur.Fragment {
			break
		}
	}
	return nil
}

func (sws *serverWatchStream) close() {
	sws.watchStream.Close()
	close(sws.closec)
	sws.wg.Wait()
}

func (sws *serverWatchStream) newResponseHeader(rev int64) *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: uint64(sws.clusterID),
		MemberId:  uint64(sws.memberID),
		Revision:  rev,
		RaftTerm:  sws.sg.Term(),
	}
}

func filterNoDelete(e mvccpb.Event) bool {
	return e.Type == mvccpb.DELETE
}

func filterNoPut(e mvccpb.Event) bool {
	return e.Type == mvccpb.PUT
}

// FiltersFromRequest returns "mvcc.FilterFunc" from a given watch create request.
func FiltersFromRequest(creq *pb.WatchCreateRequest) []mvcc.FilterFunc {
	filters := make([]mvcc.FilterFunc, 0, len(creq.Filters))
	for _, ft := range creq.Filters {
		switch ft {
		case pb.WatchCreateRequest_NOPUT:
			filters = append(filters, filterNoPut)
		case pb.WatchCreateRequest_NODELETE:
			filters = append(filters, filterNoDelete)
		default:
		}
	}
	return filters
}
