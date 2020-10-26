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

package backend

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	defaultBatchLimit    = 10000                  //默认的批量限制
	defaultBatchInterval = 100 * time.Millisecond //默认的批量间隔

	defragLimit = 10000 //碎片整理限制

	// initialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	// initialMmapSize是映射区域的初始大小。 将此值设置为大于潜在的最大db大小可以防止写入器阻塞读取器。仅适用于Linux。
	initialMmapSize = uint64(10 * 1024 * 1024 * 1024) // 10GB

	// minSnapshotWarningTimeout is the minimum threshold to trigger a long running snapshot warning.
	minSnapshotWarningTimeout = 30 * time.Second
)

type Backend interface {
	// ReadTx returns a read transaction. It is replaced by ConcurrentReadTx in the main data path, see #10523.
	// ReadTx返回读取的事务。 在主数据路径中将其替换为ConcurrentReadTx，请参见＃10523。
	ReadTx() ReadTx
	BatchTx() BatchTx
	// ConcurrentReadTx returns a non-blocking read transaction.
	// ConcurrentReadTx 返回一个 不被阻塞 的读事务
	ConcurrentReadTx() ReadTx

	Snapshot() Snapshot
	Hash(ignores map[IgnoreKey]struct{}) (uint32, error)
	// Size returns the current size of the backend physically allocated.
	// Size 返回目前backend已经分配的物理存储的大小
	// The backend can hold DB space that is not utilized at the moment,
	// since it can conduct pre-allocation or spare unused space for recycling.
	// Use SizeInUse() instead for the actual DB size.
	// backend 可以保留 当前未使用的数据库空间，因为他可以进行预分配或者保留未使用空间以进行回收。
	// 使用SizeInUse() 代替实际的数据库大小
	Size() int64
	// SizeInUse returns the current size of the backend logically in use.
	// SizeInUse 返回当前backend在逻辑上使用的大小
	// Since the backend can manage free space in a non-byte unit such as
	// number of pages, the returned value can be not exactly accurate in bytes.
	// 由于backend可以以非字节为单位（例如页数）来管理可用空间，因此返回的值可能不完全以字节为单位。
	SizeInUse() int64
	// OpenReadTxN returns the number of currently open read transactions in the backend.
	// OpenReadTxN 返回当前在backe	nd中开启的读事务的数量
	OpenReadTxN() int64
	Defrag() error
	ForceCommit()
	Close() error
}

type Snapshot interface {
	// Size gets the size of the snapshot.
	// Size 返回 snapshot 的size
	Size() int64
	// WriteTo 将 snapshot 写入到 给定的 writer 中
	// WriteTo writes the snapshot into the given writer.
	WriteTo(w io.Writer) (n int64, err error)
	// Close closes the snapshot.
	// Close 关闭 snapshot
	Close() error
}

type backend struct {
	// size and commits are used with atomic operations so they must be
	// 64-bit aligned, otherwise 32-bit tests will crash
	// size和commits与原子操作一起使用，因此它们必须是64位对齐的，否则32位测试将崩溃

	// size is the number of bytes allocated in the backend
	// size 是backend 分配的 字节数
	size int64
	// sizeInUse 是backend分配后使用的字节数
	// sizeInUse is the number of bytes actually used in the backend
	sizeInUse int64
	// commits counts number of commits since start
	// commits 统计在开始后commits的数量
	commits int64
	// openReadTxN is the number of currently open read transactions in the backend
	// 目前开启的读事务数量
	openReadTxN int64

	mu sync.RWMutex
	db *bolt.DB

	batchInterval time.Duration
	batchLimit    int
	batchTx       *batchTxBuffered //在backend成员函数中只是用到了他的commit，像put，都是backend.Backend.BatchTx 获取到实例之后，用实例去调用的。

	readTx *readTx

	stopc chan struct{}
	donec chan struct{}

	lg *zap.Logger
}

type BackendConfig struct {
	// Path is the file path to the backend file.
	// Path 数据库文件路径
	Path string
	// BatchInterval is the maximum time before flushing the BatchTx.
	BatchInterval time.Duration
	// BatchLimit is the maximum puts before flushing the BatchTx.
	BatchLimit int
	// BackendFreelistType is the backend boltdb's freelist type.
	BackendFreelistType bolt.FreelistType
	// MmapSize is the number of bytes to mmap for the backend.
	MmapSize uint64
	// Logger logs backend-side operations.
	Logger *zap.Logger
	// UnsafeNoFsync disables all uses of fsync.
	UnsafeNoFsync bool `json:"unsafe-no-fsync"`
}

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval, // 100ms
		BatchLimit:    defaultBatchLimit,    // 1w
		MmapSize:      initialMmapSize,      // 10G
	}
}

func New(bcfg BackendConfig) Backend {
	return newBackend(bcfg)
}

func NewDefaultBackend(path string) Backend {
	bcfg := DefaultBackendConfig()
	bcfg.Path = path
	return newBackend(bcfg)
}

// 初始化了一个backend，然后开了个定时任务 commit，默认100ms一次
func newBackend(bcfg BackendConfig) *backend {
	if bcfg.Logger == nil {
		bcfg.Logger = zap.NewNop()
	}

	bopts := &bolt.Options{}
	if boltOpenOptions != nil {
		*bopts = *boltOpenOptions
	}
	bopts.InitialMmapSize = bcfg.mmapSize()
	bopts.FreelistType = bcfg.BackendFreelistType
	bopts.NoSync = bcfg.UnsafeNoFsync
	bopts.NoGrowSync = bcfg.UnsafeNoFsync

	db, err := bolt.Open(bcfg.Path, 0600, bopts)
	if err != nil {
		bcfg.Logger.Panic("failed to open database", zap.String("path", bcfg.Path), zap.Error(err))
	}

	// In future, may want to make buffering optional for low-concurrency systems
	// or dynamically swap between buffered/non-buffered depending on workload.
	// 将来，可能希望对低并发系统使缓冲成为可选，或根据工作负载在缓冲/非缓冲之间动态交换。
	b := &backend{
		db: db,

		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,

		readTx: &readTx{
			baseReadTx: baseReadTx{
				buf: txReadBuffer{
					txBuffer: txBuffer{make(map[string]*bucketBuffer)},
				},
				buckets: make(map[string]*bolt.Bucket),
				txWg:    new(sync.WaitGroup),
				txMu:    new(sync.RWMutex),
			},
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),

		lg: bcfg.Logger,
	}
	b.batchTx = newBatchTxBuffered(b)
	go b.run()
	return b
}

// BatchTx returns the current batch tx in coalescer. The tx can be used for read and
// write operations. The write result can be retrieved within the same tx immediately.
// The write result is isolated with other txs until the current one get committed.
// BatchTx 返回 目前的 batch。 该tx 可以被用来执行 读写操作。写结果可以马上被检索到，在用通一个tx的情况下。
// 其他tx是无法获取到写结果的，知道 目前的batch呗提交
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *backend) ReadTx() ReadTx { return b.readTx }

// ConcurrentReadTx creates and returns a new ReadTx, which:
// A) creates and keeps a copy of backend.readTx.txReadBuffer,
// B) references the boltdb read Tx (and its bucket cache) of current batch interval.
// ConcurrentReadTx 创建并且返回一个新的ReadTx
// A) 创建并保持一份 backend.readTx.txReadBuffer的拷贝。
// B) 引用当前批处理间隔的boltdb读取的Tx（及其bucket缓存）。 就是除了 txReadBuffer 是copy以外，其他的都是原来字段的引用。
func (b *backend) ConcurrentReadTx() ReadTx {
	b.readTx.RLock()
	defer b.readTx.RUnlock()
	// prevent boltdb read Tx from been rolled back until store read Tx is done. Needs to be called when holding readTx.RLock().
	b.readTx.txWg.Add(1)
	// TODO: might want to copy the read buffer lazily - create copy when A) end of a write transaction B) end of a batch interval.
	// TODO: 可能希望在copy read buffer 的是 lazily copy [就是延后copy呗] ，A:写事务之后 B batch 间隔之后 （就那个100ms）
	return &concurrentReadTx{
		baseReadTx: baseReadTx{
			buf:     b.readTx.buf.unsafeCopy(),
			txMu:    b.readTx.txMu,
			tx:      b.readTx.tx,
			buckets: b.readTx.buckets,
			txWg:    b.readTx.txWg,
		},
	}
}

// ForceCommit forces the current batching tx to commit.
// 强制提交
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}

// db.Begin了一个事务，然后起了个协程，去等待，快照生成结束[超时报警]，自己并没有去执行快照操作。
func (b *backend) Snapshot() Snapshot {
	b.batchTx.Commit() // 先提交

	b.mu.RLock() // 加读取锁
	defer b.mu.RUnlock()
	tx, err := b.db.Begin(false) // 开启一个 boltDB事务
	if err != nil {
		b.lg.Fatal("failed to begin tx", zap.Error(err))
	}

	stopc, donec := make(chan struct{}), make(chan struct{})
	dbBytes := tx.Size()
	// 这个 go 协程中， 没有真正的去备份呀。就是开了个定时器，然后返回 stopc 的chan，等待外部 快照生成后，给stopc发消息，然后结束，结束的时候还发给普罗米修斯监控。
	go func() {
		defer close(donec)
		// sendRateBytes is based on transferring snapshot data over a 1 gigabit/s connection
		// assuming a min tcp throughput of 100MB/s.
		var sendRateBytes int64 = 100 * 1024 * 1024
		warningTimeout := time.Duration(int64((float64(dbBytes) / float64(sendRateBytes)) * float64(time.Second)))
		if warningTimeout < minSnapshotWarningTimeout {
			warningTimeout = minSnapshotWarningTimeout
		}
		start := time.Now()
		ticker := time.NewTicker(warningTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.lg.Warn(
					"snapshotting taking too long to transfer",
					zap.Duration("taking", time.Since(start)),
					zap.Int64("bytes", dbBytes),
					zap.String("size", humanize.Bytes(uint64(dbBytes))),
				)

			case <-stopc:
				snapshotTransferSec.Observe(time.Since(start).Seconds())
				return
			}
		}
	}()

	// 然后直接返回了
	return &snapshot{tx, stopc, donec}
}

type IgnoreKey struct {
	Bucket string
	Key    string
}

//取出db中每个bucket的数据，算 hash sum32
func (b *backend) Hash(ignores map[IgnoreKey]struct{}) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.mu.RLock()
	defer b.mu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error { //db.View是开启一个只读事务，读请求在参数函数中。
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("cannot get hash of bucket %s", string(next))
			}
			h.Write(next)
			b.ForEach(func(k, v []byte) error {
				bk := IgnoreKey{Bucket: string(next), Key: string(k)}
				if _, ok := ignores[bk]; !ok {
					h.Write(k)
					h.Write(v)
				}
				return nil
			})
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}

func (b *backend) run() {
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval) // batchInterval 默认100ms todo time包看一下，sort 包也看一下
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		if b.batchTx.safePending() != 0 {
			//如果有改动，就commit
			b.batchTx.Commit()
		}
		t.Reset(b.batchInterval)
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

// Commits returns total number of commits since start
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

func (b *backend) Defrag() error {
	return b.defrag()
}

func (b *backend) defrag() error {
	now := time.Now()

	// TODO: make this non-blocking?
	// lock batchTx to ensure nobody is using previous tx, and then
	// close previous ongoing tx.
	b.batchTx.Lock()
	defer b.batchTx.Unlock()

	// lock database after lock tx to avoid deadlock.
	b.mu.Lock()
	defer b.mu.Unlock()

	// block concurrent read requests while resetting tx
	b.readTx.Lock()
	defer b.readTx.Unlock()

	b.batchTx.unsafeCommit(true)

	b.batchTx.tx = nil

	// Create a temporary file to ensure we start with a clean slate.
	// Snapshotter.cleanupSnapdir cleans up any of these that are found during startup.
	dir := filepath.Dir(b.db.Path())
	temp, err := ioutil.TempFile(dir, "db.tmp.*")
	if err != nil {
		return err
	}
	options := bolt.Options{}
	if boltOpenOptions != nil {
		options = *boltOpenOptions
	}
	options.OpenFile = func(path string, i int, mode os.FileMode) (file *os.File, err error) {
		return temp, nil
	}
	tdbp := temp.Name()
	tmpdb, err := bolt.Open(tdbp, 0600, &options)
	if err != nil {
		return err
	}

	dbp := b.db.Path()
	size1, sizeInUse1 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"defragmenting",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes", size1),
			zap.String("current-db-size", humanize.Bytes(uint64(size1))),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse1),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse1))),
		)
	}
	// gofail: var defragBeforeCopy struct{}
	err = defragdb(b.db, tmpdb, defragLimit)
	if err != nil {
		tmpdb.Close()
		if rmErr := os.RemoveAll(tmpdb.Path()); rmErr != nil {
			b.lg.Error("failed to remove db.tmp after defragmentation completed", zap.Error(rmErr))
		}
		return err
	}

	err = b.db.Close()
	if err != nil {
		b.lg.Fatal("failed to close database", zap.Error(err))
	}
	err = tmpdb.Close()
	if err != nil {
		b.lg.Fatal("failed to close tmp database", zap.Error(err))
	}
	// gofail: var defragBeforeRename struct{}
	err = os.Rename(tdbp, dbp)
	if err != nil {
		b.lg.Fatal("failed to rename tmp database", zap.Error(err))
	}

	b.db, err = bolt.Open(dbp, 0600, boltOpenOptions)
	if err != nil {
		b.lg.Fatal("failed to open database", zap.String("path", dbp), zap.Error(err))
	}
	b.batchTx.tx = b.unsafeBegin(true)

	b.readTx.reset()
	b.readTx.tx = b.unsafeBegin(false)

	size := b.readTx.tx.Size()
	db := b.readTx.tx.DB()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	took := time.Since(now)
	defragSec.Observe(took.Seconds())

	size2, sizeInUse2 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"defragmented",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes-diff", size2-size1),
			zap.Int64("current-db-size-bytes", size2),
			zap.String("current-db-size", humanize.Bytes(uint64(size2))),
			zap.Int64("current-db-size-in-use-bytes-diff", sizeInUse2-sizeInUse1),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse2),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse2))),
			zap.Duration("took", took),
		)
	}
	return nil
}

func defragdb(odb, tmpdb *bolt.DB, limit int) error {
	// open a tx on tmpdb for writes
	tmptx, err := tmpdb.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tmptx.Rollback()
		}
	}()

	// open a tx on old db for read
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	c := tx.Cursor()

	count := 0
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		tmpb.FillPercent = 0.9 // for seq write in for each

		if err = b.ForEach(func(k, v []byte) error {
			count++
			if count > limit {
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				tmpb.FillPercent = 0.9 // for seq write in for each

				count = 0
			}
			return tmpb.Put(k, v)
		}); err != nil {
			return err
		}
	}

	return tmptx.Commit()
}

func (b *backend) begin(write bool) *bolt.Tx {
	b.mu.RLock()
	tx := b.unsafeBegin(write)
	b.mu.RUnlock()

	size := tx.Size()
	db := tx.DB()
	stats := db.Stats()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(stats.FreePageN)*int64(db.Info().PageSize)))
	atomic.StoreInt64(&b.openReadTxN, int64(stats.OpenTxN))

	return tx
}

func (b *backend) unsafeBegin(write bool) *bolt.Tx {
	tx, err := b.db.Begin(write)
	if err != nil {
		b.lg.Fatal("failed to begin tx", zap.Error(err))
	}
	return tx
}

func (b *backend) OpenReadTxN() int64 {
	return atomic.LoadInt64(&b.openReadTxN)
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(batchInterval time.Duration, batchLimit int) (*backend, string) {
	dir, err := ioutil.TempDir(os.TempDir(), "etcd_backend_test")
	if err != nil {
		panic(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg := DefaultBackendConfig()
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = tmpPath, batchInterval, batchLimit
	return newBackend(bcfg), tmpPath
}

func NewDefaultTmpBackend() (*backend, string) {
	return NewTmpBackend(defaultBatchInterval, defaultBatchLimit)
}

type snapshot struct {
	*bolt.Tx
	stopc chan struct{}
	donec chan struct{}
}

func (s *snapshot) Close() error {
	close(s.stopc)
	<-s.donec
	return s.Tx.Rollback()
}
