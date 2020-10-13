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

package wal

import (
	"errors"
	"fmt"
	"strings"

	"go.etcd.io/etcd/v3/pkg/fileutil"

	"go.uber.org/zap"
)

var errBadWALName = errors.New("bad wal name")

// Exist returns true if there are any files in a given directory.
// 判断给定的文件夹中是否有 .wal文件，etcd默认的看 default.etcd/member/wal 下是否有.wal为扩展名的字段。
func Exist(dir string) bool {
	names, err := fileutil.ReadDir(dir, fileutil.WithExt(".wal"))
	if err != nil {
		return false
	}
	return len(names) != 0
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
//searchIndex 返回 raft index section 等于或小于给定索引的names参数的的最后一个数组索引。
//参数 names 必须是有序的
// 就是比如有0-1.wal 0-2.wal 0-3.wal 0-4.wal ，传入的index是3,那么就会返回index=2。
// 如果有0-1.wal 0-2.wal 0-4.wal，传入的是3，那么index返回的是1.就是0-2.wal的索引
func searchIndex(lg *zap.Logger, names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWALName(name)
		if err != nil {
			lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

// names should have been sorted based on sequence number.
// 参数names应该根据序号排序。
// isValidSeq checks whether seq increases continuously.
// isValidSeq检查seq是否递增。
func isValidSeq(lg *zap.Logger, names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWALName(name)
		if err != nil {
			lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

func readWALNames(lg *zap.Logger, dirpath string) ([]string, error) {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}
	wnames := checkWalNames(lg, names)
	if len(wnames) == 0 {
		return nil, ErrFileNotFound
	}
	return wnames, nil
}

//检查给定的names参数是否是 xxxx-xxxxx.wal 格式的。返回符合格式的names，是不是叫filterNotWalNames更好。
func checkWalNames(lg *zap.Logger, names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseWALName(name); err != nil {
			// don't complain about left over tmp files
			if !strings.HasSuffix(name, ".tmp") {
				lg.Warn(
					"ignored file in WAL directory",
					zap.String("path", name),
				)
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

//解析wal名称，返回xxx1-xxx2.wal中的xxx1和xxx2,如果不符合这种格式返回err
func parseWALName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errBadWALName
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

//生成wal name
func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}
