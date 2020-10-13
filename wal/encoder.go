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
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"go.etcd.io/etcd/v3/pkg/crc"
	"go.etcd.io/etcd/v3/pkg/ioutil"
	"go.etcd.io/etcd/v3/wal/walpb"
)

// walPageBytes is the alignment for flushing records to the backing Writer.
// walPageBytes是将记录刷新到后备Writer的对齐方式。
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
// 它应该是最小扇区大小的倍数，以便WAL可以安全地区分写入中断和普通数据损坏。
const walPageBytes = 8 * minSectorSize

type encoder struct {
	mu sync.Mutex
	bw *ioutil.PageWriter

	crc       hash.Hash32
	buf       []byte
	uint64buf []byte
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc: crc.New(prevCrc, crcTable),
		// 1MB buffer
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// newFileEncoder creates a new encoder with current file offset for the page writer.
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()
	var (
		data []byte
		err  error
		n    int
	)

	if rec.Size() > len(e.buf) {
		data, err = rec.Marshal()
		if err != nil {
			return err
		}
	} else {
		n, err = rec.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}

	lenField, padBytes := encodeFrameSize(len(data))
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}

	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...) //8字节对其，后面填充空byte
	}
	// 将data写入。前面已经写入了 该data的长度，现在写入 data 。 符合len+data的传输规则。
	n, err = e.bw.Write(data)
	walWriteBytes.Add(float64(n)) //将写入的字节数传给普罗米修斯，
	return err
}

//参数dataBytes是字节数。该函数计算传入大小的8字节对其后的大小
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	// 强制8字节对齐，因此长度永远不会被撕裂
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		//0x80 = 1000 0000 (二进制)
		//padBytes是0-7（二进制000-111）,肯定是三位。或上 0x80 也就是最高位是1，最低三位是0-7
		//左移56位，又是uint64类型，那么右边表达式高8位就是 1000 0xxx 0...(56个0)
		//lenField最终又或上该值，大概率原lenField就是某个长度，放在低56位上。最终的lenField高8位代表强制对其的pad字节数；低56位代表原始的dataBytes字节数。
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

func (e *encoder) flush() error {
	e.mu.Lock()
	n, err := e.bw.FlushN()
	e.mu.Unlock()
	walWriteBytes.Add(float64(n))
	return err
}

//以小端格式写入，将n放在buf中，并写入io中
func writeUint64(w io.Writer, n uint64, buf []byte) error {
	// http://golang.org/src/encoding/binary/binary.go
	binary.LittleEndian.PutUint64(buf, n)
	nv, err := w.Write(buf)
	walWriteBytes.Add(float64(nv))
	return err
}
