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

package mvcc

import (
	"encoding/binary"
	"time"

	"go.uber.org/zap"
)

// 它删除了 compactMainRev 以及compactMainRev之前的所有的key。然后遇到keep中的revision会忽略，不删除。最后记录了一个 bucket：meta ，key：finishedCompactRev， value：compactMainRev的值
func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	totalStart := time.Now()
	defer func() { dbCompactionTotalMs.Observe(float64(time.Since(totalStart) / time.Millisecond)) }() // 监控压缩时长
	keyCompactions := 0
	defer func() { dbCompactionKeysCounter.Add(float64(keyCompactions)) }() //监控压缩的key的个数？

	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1)) //压缩的应该是到end，但是不包括end，所以这里加了个1

	last := make([]byte, 8+1+8) //空的就是第一个嘛类似 0_0 这种
	for {
		var rev revision

		start := time.Now()

		tx := s.b.BatchTx()
		tx.Lock()
		keys, _ := tx.UnsafeRange(keyBucketName, last, end, int64(s.cfg.CompactionBatchLimit)) //取出范围内最多 CompactionBatchLimit 个 key。
		for _, key := range keys {
			rev = bytesToRev(key)        //将byte转化为revision
			if _, ok := keep[rev]; !ok { //如果没有要求保留，就直接删除
				tx.UnsafeDelete(keyBucketName, key) // 删除
				keyCompactions++                    // 压缩计数增加
			}
		}

		if len(keys) < s.cfg.CompactionBatchLimit { //证明已经取尽了
			rbytes := make([]byte, 8+1+8)
			revToBytes(revision{main: compactMainRev}, rbytes)
			tx.UnsafePut(metaBucketName, finishedCompactKeyName, rbytes) // 在meta bucket中记录了一下，目前已经压缩到哪个revision了。
			tx.Unlock()
			s.lg.Info(
				"finished scheduled compaction",
				zap.Int64("compact-revision", compactMainRev),
				zap.Duration("took", time.Since(totalStart)),
			)
			return true
		}

		// update last
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last) // last 被赋值为新的rev
		tx.Unlock()
		// Immediately commit the compaction deletes instead of letting them accumulate in the write buffer
		// 立即提交压缩删除，而不是让它们堆积在写缓冲区中
		s.b.ForceCommit()
		dbCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))

		select {
		case <-time.After(10 * time.Millisecond): //10ms后继续，速率控制
		case <-s.stopc:
			return false
		}
	}
}
