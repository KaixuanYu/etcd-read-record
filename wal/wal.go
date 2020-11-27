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
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/v3/pkg/fileutil"
	"go.etcd.io/etcd/v3/pkg/pbutil"
	"go.etcd.io/etcd/v3/raft"
	"go.etcd.io/etcd/v3/raft/raftpb"
	"go.etcd.io/etcd/v3/wal/walpb"

	"go.uber.org/zap"
)

const (
	//wal文件中可以存的类型
	metadataType int64 = iota + 1
	entryType
	stateType
	crcType
	snapshotType

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	// warnSyncDuration是记录警告之前分配给fsync的时间
	warnSyncDuration = time.Second
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// SegmentSizeBytes是每个wal段文件的预分配大小。
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	// 实际大小可能大于此。 通常，应该使用默认值，但是将其定义为导出变量，以便测试可以设置不同的段大小。
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	ErrMetadataConflict             = errors.New("wal: conflicting metadata found")
	ErrFileNotFound                 = errors.New("wal: file not found")
	ErrCRCMismatch                  = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch             = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound             = errors.New("wal: snapshot not found")
	ErrSliceOutOfRange              = errors.New("wal: slice bounds out of range")
	ErrMaxWALEntrySizeLimitExceeded = errors.New("wal: max entry size limit exceeded")
	ErrDecoderNotFound              = errors.New("wal: decoder not found")
	crcTable                        = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL是永久存储的逻辑表示
// WAL is either in read mode or append mode but not both.
// WAL处于读取模式或附加模式，但不是两者都处于。
// A newly created WAL is in append mode, and ready for appending records.
// 新创建的WAL处于附加模式，并准备附加记录。
// A just opened WAL is in read mode, and ready for reading records.
// 刚打开的WAL处于读取模式，并准备读取记录。
// The WAL will be ready for appending after reading out all the previous records.
// 读出所有先前的记录后，WAL将准备好附加
type WAL struct {
	lg *zap.Logger

	dir string // the living directory of the underlay files // 默认情况下会在etcd目录下生成一个default.etcd/member/wal的目录

	// dirFile is a fd for the wal directory for syncing on Rename
	// dirFile是wal目录的fd，用于在重命名时同步
	dirFile *os.File

	metadata []byte           // metadata recorded at the head of each WAL [metadata 被记录到每个WAL文件的头部[
	state    raftpb.HardState // hardstate recorded at the head of WAL [hardstate 被记录到每个WAL文件的头部]

	start     walpb.Snapshot // snapshot to start reading 开始阅读的快照
	decoder   *decoder       // decoder to decode records 解码records的解码器
	readClose func() error   // closer for decode reader 会关闭decode中的所有reader

	unsafeNoSync bool // if set, do not fsync 开了可能会丢数据

	//todo mu锁定的是encoder？这个锁到底锁定啥的？锁locks文件？
	// 锁 锁定的 不一定是资源。 这里的mu锁定的是一些互斥的操作。加锁的函数都是对外的可导出的。这些操作都是互斥的，不允许同时操作。完美。
	mu      sync.Mutex
	enti    uint64   // index of the last entry saved to the wal 储存在wal中最后一个entry的索引
	encoder *encoder // encoder to encode records 编码records的编码器

	locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing) wal持有的锁定文件（名称不断增加）
	// 分配磁盘空间的管道。这个在etcd启动的时候跑了个协程创建一个0.tmp或者1.tmp文件。
	// 就是为了当需要重新创建个xxx-xxx.wal的时候不用从磁盘申请，而是直接将.tmp文件转换成xxx-xxx.wal文件
	// 因为它是异步创建的文件，每次都会有一个.tmp文件
	fp *filePipeline
}

// Create creates a WAL ready for appending records. The given metadata is
// recorded at the head of each WAL file, and can be retrieved with ReadAll
// after the file is Open.
// Create 函数创建了一个WAL，用来附加记录（records）。
// 参数中传进来的 metadata 参数会被记录到每个WAL文件的头部。在打开文件后可以用ReadAll函数检索到。
func Create(lg *zap.Logger, dirpath string, metadata []byte) (*WAL, error) {
	if Exist(dirpath) { //判断default.etcd/member/wal目录下是否有文件
		return nil, os.ErrExist
	}

	if lg == nil {
		lg = zap.NewNop()
	}

	// keep temporary wal directory so WAL initialization appears atomic
	// 为了wal初始化的原子性，保留一个临时wal文件
	tmpdirpath := filepath.Clean(dirpath) + ".tmp"
	if fileutil.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	//创建 default.etcd/member/wal.tmp 目录
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		lg.Warn(
			"failed to create a temporary WAL directory",
			zap.String("tmp-dir-path", tmpdirpath),
			zap.String("dir-path", dirpath),
			zap.Error(err),
		)
		return nil, err
	}

	p := filepath.Join(tmpdirpath, walName(0, 0))                                     //p=default.etcd/member/wal/0000000000000000-0000000000000000.wal 文件
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode) //os.O_WRONLY 只写 ，os.O_CREATE 不存在就创建。
	if err != nil {
		lg.Warn(
			"failed to flock an initial WAL file",
			zap.String("path", p),
			zap.Error(err),
		)
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		lg.Warn(
			"failed to seek an initial WAL file",
			zap.String("path", p),
			zap.Error(err),
		)
		return nil, err
	}
	//预开辟文件空间，64MB
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
		lg.Warn(
			"failed to preallocate an initial WAL file",
			zap.String("path", p),
			zap.Int64("segment-bytes", SegmentSizeBytes),
			zap.Error(err),
		)
		return nil, err
	}

	w := &WAL{
		lg:       lg,
		dir:      dirpath,
		metadata: metadata,
	}
	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	w.locks = append(w.locks, f)
	//存入了一个crc的wal records
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}
	//存入一个metadata类型的 wal records
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	//存入一个 snapshot 类型的 wal records
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
		return nil, err
	}

	logDirPath := w.dir
	//写前几个类型的records的时候都是写到wal.tmp目录中的，然后从这里将wal.tmp目录改成wal目录
	if w, err = w.renameWAL(tmpdirpath); err != nil {
		lg.Warn(
			"failed to rename the temporary WAL directory",
			zap.String("tmp-dir-path", tmpdirpath),
			zap.String("dir-path", logDirPath),
			zap.Error(err),
		)
		return nil, err
	}

	var perr error
	defer func() {
		if perr != nil {
			w.cleanupWAL(lg)
		}
	}()

	// directory was renamed; sync parent dir to persist rename
	// 目录已重命名；同步父目录以保持重命名
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	if perr != nil {
		lg.Warn(
			"failed to open the parent data directory",
			zap.String("parent-dir-path", filepath.Dir(w.dir)),
			zap.String("dir-path", w.dir),
			zap.Error(perr),
		)
		return nil, perr
	}
	dirCloser := func() error {
		if perr = pdir.Close(); perr != nil {
			lg.Warn(
				"failed to close the parent data directory file",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
			return perr
		}
		return nil
	}
	start := time.Now()
	//Fsync 将高速缓存中的数据立刻刷到磁盘上。 这刷的好像是文件目录
	if perr = fileutil.Fsync(pdir); perr != nil {
		dirCloser()
		lg.Warn(
			"failed to fsync the parent data directory file",
			zap.String("parent-dir-path", filepath.Dir(w.dir)),
			zap.String("dir-path", w.dir),
			zap.Error(perr),
		)
		return nil, perr
	}
	//刷新到磁盘的时间间隔上传到普罗米修斯
	walFsyncSec.Observe(time.Since(start).Seconds())
	if err = dirCloser(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WAL) SetUnsafeNoFsync() {
	w.unsafeNoSync = true
}

func (w *WAL) cleanupWAL(lg *zap.Logger) {
	var err error
	if err = w.Close(); err != nil {
		lg.Panic("failed to close WAL during cleanup", zap.Error(err))
	}
	//将 default.etcd/member/wal 改成 default.etcd/member/wal.broken.$(time)
	brokenDirName := fmt.Sprintf("%s.broken.%v", w.dir, time.Now().Format("20060102.150405.999999"))
	if err = os.Rename(w.dir, brokenDirName); err != nil {
		lg.Panic(
			"failed to rename WAL during cleanup",
			zap.Error(err),
			zap.String("source-path", w.dir),
			zap.String("rename-path", brokenDirName),
		)
	}
}

func (w *WAL) renameWAL(tmpdirpath string) (*WAL, error) {
	if err := os.RemoveAll(w.dir); err != nil {
		return nil, err
	}
	// On non-Windows platforms, hold the lock while renaming. Releasing
	// the lock and trying to reacquire it quickly can be flaky because
	// it's possible the process will fork to spawn a process while this is
	// happening. The fds are set up as close-on-exec by the Go runtime,
	// but there is a window between the fork and the exec where another
	// process holds the lock.
	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		if _, ok := err.(*os.LinkError); ok {
			return w.renameWALUnlock(tmpdirpath)
		}
		return nil, err
	}
	w.fp = newFilePipeline(w.lg, w.dir, SegmentSizeBytes)
	df, err := fileutil.OpenDir(w.dir)
	w.dirFile = df
	return w, err
}

func (w *WAL) renameWALUnlock(tmpdirpath string) (*WAL, error) {
	// rename of directory with locked files doesn't work on windows/cifs;
	// close the WAL to release the locks so the directory can be renamed.
	w.lg.Info(
		"closing WAL to release flock and retry directory renaming",
		zap.String("from", tmpdirpath),
		zap.String("to", w.dir),
	)
	w.Close()

	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}

	// reopen and relock
	newWAL, oerr := Open(w.lg, w.dir, walpb.Snapshot{})
	if oerr != nil {
		return nil, oerr
	}
	if _, _, _, err := newWAL.ReadAll(); err != nil {
		newWAL.Close()
		return nil, err
	}
	return newWAL, nil
}

// Open opens the WAL at the given snap. 在给定的快照上开启WAL
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// 该快照应先前已保存到WAL，否则的话以下ReadAll将失败。
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
// 返回值的WAL 是可以 读 的，并且读出来的第一条记录是给定的snap之后的那个。
// 在读取完所有之前records之前wal是不可以再附加的，就不是可以写入呗。
func Open(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(lg, dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(lg, dirpath, snap, false)
}

// 返回了一个新的WAL，新的wal中有snap之后的所有的.wal文件。可读，是否可写根据参数write指定
func openAtIndex(lg *zap.Logger, dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	//返回的names是所有的.wal文件，nameIndex是snap之后的那个。
	//比如有返回的names有 1-1.wal 1-3.wal 1-4.wal -后面的数字就是index，snap中有个index字段，跟这个index是一样的。
	// 如果传入的snap的index=2，那么nameIndex=0，是1-1.wal的索引。如果有1-2.wal文件会返回该文件的索引。
	names, nameIndex, err := selectWALFiles(lg, dirpath, snap)
	if err != nil {
		return nil, err
	}

	//返回的rs是reader集合 ls是lockedFile集合 closer是一个关闭所有文件的函数
	rs, ls, closer, err := openWALFiles(lg, dirpath, names, nameIndex, write)
	if err != nil {
		return nil, err
	}

	// create a WAL ready for reading
	w := &WAL{
		lg:        lg,
		dir:       dirpath,
		start:     snap,
		decoder:   newDecoder(rs...),
		readClose: closer,
		locks:     ls,
	}

	if write {
		// write reuses the file descriptors from read; don't close so
		// WAL can append without dropping the file lock
		//write重用read中的文件描述符；所以不要关闭
		//WAL可以在不删除文件锁的情况下进行追加
		w.readClose = nil
		if _, _, err := parseWALName(filepath.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		w.fp = newFilePipeline(lg, w.dir, SegmentSizeBytes)
	}

	return w, nil
}

//返回dirpath中的所有.wal文件，和snap所在的.wal文件的索引。
func selectWALFiles(lg *zap.Logger, dirpath string, snap walpb.Snapshot) ([]string, int, error) {
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, -1, err
	}

	nameIndex, ok := searchIndex(lg, names, snap.Index)
	if !ok || !isValidSeq(lg, names[nameIndex:]) {
		err = ErrFileNotFound
		return nil, -1, err
	}

	return names, nameIndex, nil
}

/* 打开WAL文件。返回所有nameIndex之后的names中的文件。可写的话会返回[]lockedFile
/* dirpath 是文件目录 default.etcd/member/wal
/* names 是wal目录中所有的xxx-xxx.wal文件
/* nameIndex 是names数组中的一个索引
/* write 是否可写
*/
func openWALFiles(lg *zap.Logger, dirpath string, names []string, nameIndex int, write bool) ([]io.Reader, []*fileutil.LockedFile, func() error, error) {
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	ls := make([]*fileutil.LockedFile, 0)
	for _, name := range names[nameIndex:] { //遍历nameIndex已经之后的文件
		p := filepath.Join(dirpath, name)
		if write {
			//以读写方式打开文件
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(lg, rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, l)
			rcs = append(rcs, l)
		} else {
			//以只读方式打开文件
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(lg, rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, nil) //只读不锁定文件
			rcs = append(rcs, rf)
		}
		rs = append(rs, rcs[len(rcs)-1])
	}

	closer := func() error { return closeAll(lg, rcs...) }

	return rs, ls, closer, nil
}

// ReadAll reads out records of the current WAL.
// ReadAll 读取当前WAL的所有的记录
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// 如果是以写权限打开的wal，必须read完所有的记录，知道EOF。否则的话，会返回error
// If opened in read mode, it will try to read all records if possible. //如果是读权限打开，他会尽可能的读出全部记录
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound. //如果没有读到预期的snap，会返回 ErrSnapshotNotFound
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records. 在调用ReadAll之后，该WAL就可以附加新的records了。
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{}

	if w.decoder == nil {
		return nil, state, nil, ErrDecoderNotFound
	}
	decoder := w.decoder

	var match bool
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType:
			e := mustUnmarshalEntry(rec.Data)
			// 0 <= e.Index-w.start.Index - 1 < len(ents)
			if e.Index > w.start.Index {
				// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
				up := e.Index - w.start.Index - 1
				if up > uint64(len(ents)) {
					// return error before append call causes runtime panic
					return nil, state, nil, ErrSliceOutOfRange
				}
				ents = append(ents[:up], e)
			}
			w.enti = e.Index

		case stateType:
			state = mustUnmarshalState(rec.Data)

		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			metadata = rec.Data

		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				return nil, state, nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)

		case snapshotType:
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data)
			if snap.Index == w.start.Index {
				if snap.Term != w.start.Term {
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
				match = true
			}

		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	switch w.tail() {
	case nil:
		//如果lockedFile是空，证明是只读的wal
		// 在只读模式下，全部读出数据是没必要的。
		// We do not have to read out all entries in read mode.
		// The last record maybe a partial written one, so
		// ErrunexpectedEOF might be returned.
		//最后一条记录可能是部分写入的记录，因此可能会返回ErrunexpectedEOF。
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			state.Reset()
			return nil, state, nil, err
		}
	default:
		//有lockedFile，证明wal是可写的。
		//这时候要求必须读完所有的条目
		// We must read all of the entries if WAL is opened in write mode.
		if err != io.EOF {
			state.Reset()
			return nil, state, nil, err
		}
		// decodeRecord() will return io.EOF if it detects a zero record,
		// but this zero record may be followed by non-zero records from
		// a torn write. Overwriting some of these non-zero records, but
		// not all, will cause CRC errors on WAL open. Since the records
		// were never fully synced to disk in the first place, it's safe
		// to zero them out to avoid any CRC errors from new writes.
		//如果检测到零条记录，则encodeRecord（）将返回io.EOF，但是该零条记录后面可能有来自零位写入的非零记录。
		//覆盖其中一些非零记录（但不是全部）将在WAL打开时导致CRC错误。
		//由于记录从一开始就从未完全同步到磁盘，因此可以安全地将它们清零以避免新写入产生的任何CRC错误。
		if _, err = w.tail().Seek(w.decoder.lastOffset(), io.SeekStart); err != nil {
			return nil, state, nil, err
		}
		if err = fileutil.ZeroToEnd(w.tail().File); err != nil {
			return nil, state, nil, err
		}
	}

	err = nil
	if !match {
		err = ErrSnapshotNotFound
	}

	// close decoder, disable reading
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}
	w.start = walpb.Snapshot{}

	w.metadata = metadata

	if w.tail() != nil {
		// create encoder (chain crc with the decoder), enable appending
		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}
	w.decoder = nil

	return metadata, state, ents, err
}

// ValidSnapshotEntries returns all the valid snapshot entries in the wal logs in the given directory.
// Snapshot entries are valid if their index is less than or equal to the most recent committed hardstate.
// ValiedSnapshotEntries 返回 给定walDir文件中wal logs中的 所有 合法 snapshot
// 返回的是所有wal中没有commit的snapshot
// so：实际就是拿到 wal 文件中 没有提交的所有 walpb.Snapshot 类型
func ValidSnapshotEntries(lg *zap.Logger, walDir string) ([]walpb.Snapshot, error) {
	var snaps []walpb.Snapshot
	var state raftpb.HardState
	var err error

	rec := &walpb.Record{}
	//找到所有的wal目录下所有的.wal文件名
	names, err := readWALNames(lg, walDir)
	if err != nil {
		return nil, err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	// 以 read mode 打开 wal files， 所以其他地方以 write mode 打开同一个wal文件就不会冲突了。
	rs, _, closer, err := openWALFiles(lg, walDir, names, 0, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	// 创建一个解析器，从wal 文件中读数据
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			snaps = append(snaps, loadedSnap)
		case stateType:
			state = mustUnmarshalState(rec.Data)
		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		}
	}
	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	// 由于解码器在读取模式下打开，因此我们不必读出所有WAL条目。
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	// filter out any snaps that are newer than the committed hardstate
	// 过滤掉所有比committed hardstate 新的 snaps
	n := 0
	for _, s := range snaps {
		if s.Index <= state.Commit { // 需要看下snap的index和state的commit是如何更新的。
			snaps[n] = s
			n++
		}
	}
	snaps = snaps[:n:n] //两个冒号，的意思是 返回的snaps 是原snaps的0到n索引，然后设置snaps的cap=n。就是如果一开始开辟大了，把cap缩回来。

	return snaps, nil
}

// Verify reads through the given WAL and verifies that it is not corrupted.
// 验证读取给定的WAL，并验证它没有损坏。
// It creates a new decoder to read through the records of the given WAL.
// 他 创建一个新的 decoder 来从给定的 WAL 中读取 records。
// It does not conflict with any open WAL, but it is recommended not to
// call this function after opening the WAL for writing.
// 他不与任何打开的 WAL 冲突， 但是建议不要在开发 WAL 进行写入后调用次函数。
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// 如果无法读出预期的快照，它将返回 ErrSnapshotNotFound
// If the loaded snap doesn't match with the expected one, it will
// return error ErrSnapshotMismatch.
// 如果加载的快照与预期的快照不匹配，它将返回错误ErrSnapshotMismatch。
func Verify(lg *zap.Logger, walDir string, snap walpb.Snapshot) error {
	var metadata []byte
	var err error
	var match bool

	rec := &walpb.Record{}

	if lg == nil {
		lg = zap.NewNop()
	}
	names, nameIndex, err := selectWALFiles(lg, walDir, snap)
	if err != nil {
		return err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, nameIndex, false)
	if err != nil {
		return err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case metadataType:
			//为啥上一个metadata和当前的metadata不相等会报错metadata冲突啊？不是应该相等才冲突？还是说同一个member的metadata一定相同？
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				return ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			// Current crc of decoder must match the crc of the record.
			// We need not match 0 crc, since the decoder is a new one at this point.
			// crc == 0的时候是第一个wal文件。不等于0的时候 当前crc应该等于上一个文件的crc
			// 这里有crc32的一个调用周期。可以细看一下。
			if crc != 0 && rec.Validate(crc) != nil {
				return ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			if loadedSnap.Index == snap.Index {
				if loadedSnap.Term != snap.Term {
					return ErrSnapshotMismatch
				}
				match = true
			}
		// We ignore all entry and state type records as these
		// are not necessary for validating the WAL contents
		case entryType:
		case stateType:
		default:
			return fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}

	if !match {
		return ErrSnapshotNotFound
	}

	return nil
}

// cut closes current file written and creates a new one ready to append.
// cut 关闭当前的可写file 并且创建一个新的file去追加
// cut first creates a temp wal file and writes necessary headers into it.
// cut 首先创建一个临时的wal文件，并且在文件中写一些必要的头
// Then cut atomically rename temp wal file to a wal file.
// 然后 该函数 自动的将临时wal文件重新命名为 wal 文件
// todo 骚操作挺多：etcd刚启动的时候wal文件夹下面有个 000000000000000-000000000000000.wal 和 0.tmp 文件，
// 这个cut函数，结束了对 000000000000000-000000000000000.wal 的追加，然后建一个新的  000000000000001-00000000000000x.wal 文件
// 但是它不新建文件（因为慢）,用异步生成的0.tmp文件，然后把它重名成 000000000000001-00000000000000x.wal
// 0.tmp 重名名之后，又会多一个 1.tmp 文件作为下一次cut用。
// todo 为啥不是重建一个0.tmp?因为是异步的，怕冲突。
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	off, serr := w.tail().Seek(0, io.SeekCurrent)
	if serr != nil {
		return serr
	}

	//将文件的大小更改为目前存的数据的大小
	if err := w.tail().Truncate(off); err != nil {
		return err
	}

	//更改保存到磁盘
	if err := w.sync(); err != nil {
		return err
	}

	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	w.locks = append(w.locks, newTail)
	prevCrc := w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}

	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}

	if err = w.saveState(&w.state); err != nil {
		return err
	}

	// atomically move temp wal file to wal file
	if err = w.sync(); err != nil {
		return err
	}

	off, err = w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	start := time.Now()
	if err = fileutil.Fsync(w.dirFile); err != nil {
		return err
	}
	walFsyncSec.Observe(time.Since(start).Seconds())

	// reopen newTail with its new path so calls to Name() match the wal filename format
	newTail.Close()

	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return err
	}

	w.locks[len(w.locks)-1] = newTail

	prevCrc = w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	w.lg.Info("created a new WAL segment", zap.String("path", fpath))
	return nil
}

//encoder.flush() 然后 fileutil.Fdatasync(w.tail().File)
// 其实就是对最后一个lockFile文件做 file.Write 和 file.Sync() 操作
// 为啥先 file.Write？ 因为encoder的pageWriter有个缓存，需要把缓存数据Write到系统内核，然后Sync把系统内核缓存中的数据刷到磁盘上。
func (w *WAL) sync() error {
	if w.unsafeNoSync {
		return nil
	}
	if w.encoder != nil {
		// encoder中的 ioutil.PageWriter 的 io.Writer 就是 w.tail().File
		// 这里的flush，是调用的w.tail().File.Write()
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}
	start := time.Now()
	err := fileutil.Fdatasync(w.tail().File)

	took := time.Since(start)
	if took > warnSyncDuration {
		w.lg.Warn(
			"slow fdatasync",
			zap.Duration("took", took),
			zap.Duration("expected-duration", warnSyncDuration),
		)
	}
	walFsyncSec.Observe(took.Seconds())

	return err
}

func (w *WAL) Sync() error {
	return w.sync()
}

// ReleaseLockTo releases the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
// 释放 w.locks
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.locks) == 0 {
		return nil
	}

	var smaller int
	found := false
	for i, l := range w.locks {
		_, lockIndex, err := parseWALName(filepath.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	// if no lock index is greater than the release index, we can
	// release lock up to the last one(excluding).
	if !found {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

// Close closes the current WAL file and directory.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			w.lg.Error("failed to close WAL", zap.Error(err))
		}
	}

	return w.dirFile.Close()
}

// 保存一条entryType类型的record，并且更新enti
func (w *WAL) saveEntry(e *raftpb.Entry) error {
	// TODO: add MustMarshalTo to reduce one allocation. 可以看下encoder中的buf，很细，一样的原理
	//  添加MustMarshalTo以减少一种分配。 意思是 var b *raftpb.Entry ; e.MustMarshalTo(b) ?
	//  这样只在MustMarshalTo中分配一次，不会再返回分配给b？ 的确，可以有个开辟好的buf存，然后每次都marshal到buf中，然后再复制给b 666
	b := pbutil.MustMarshal(e)
	rec := &walpb.Record{Type: entryType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	w.enti = e.Index
	return nil
}

// 保存一条 stateType 类型的记录。并更新 w.state 。
func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	w.state = *s
	b := pbutil.MustMarshal(s)
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

// 写ents， 写 st ，然后cut或者sync或者什么都不做
func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	//这是锁的整个操作？Save函数单例？
	w.mu.Lock()
	defer w.mu.Unlock()

	// short cut, do not call sync
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}

	mustSync := raft.MustSync(st, w.state, len(ents))

	// TODO(xiangli): no more reference operator

	// 将所有的raftpb.Entry存入wal
	for i := range ents {
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}
	// 存 raftpb.HardState
	if err := w.saveState(&st); err != nil {
		return err
	}

	curOff, err := w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if curOff < SegmentSizeBytes {
		if mustSync {
			return w.sync()
		}
		return nil
	}
	// 如果文件大小已经大于 64M，就cut
	return w.cut()
}

// 保存一条snapshot 记录，并且更新w.enti， 然后sync刷新到磁盘（为啥这里要sync）
func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	b := pbutil.MustMarshal(&e)

	//这是给谁加的锁？
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// update enti only when snapshot is ahead of last index
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync()
}

//往wal文件插入一条crcType的record(记录)，一般都是每个xxx-xxx.wal文件的第一个record
func (w *WAL) saveCrc(prevCrc uint32) error {
	//这没对encoder加锁啊，那w.mu锁是锁啥的？
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

//返回w.locks的最后一个，如果没有返回nil，最后一个一般就是数据需要追加的wal文件
func (w *WAL) tail() *fileutil.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1]
	}
	return nil
}

// 返回 wal 文件 xxxx-yyyy.wal 的 xxxx
func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWALName(filepath.Base(t.Name()))
	if err != nil {
		w.lg.Fatal("failed to parse WAL name", zap.String("name", t.Name()), zap.Error(err))
	}
	return seq
}

//就是依次调用参数rcs的Close函数，如果有错误，把所有的错误返回，如果没错误就返回nil
func closeAll(lg *zap.Logger, rcs ...io.ReadCloser) error {
	stringArr := make([]string, 0)
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			lg.Warn("failed to close: ", zap.Error(err))
			stringArr = append(stringArr, err.Error())
		}
	}
	if len(stringArr) == 0 {
		return nil
	}
	return errors.New(strings.Join(stringArr, ", "))
}
