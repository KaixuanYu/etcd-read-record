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
	mu sync.Mutex //锁的是 PageWriter
	bw *ioutil.PageWriter

	crc hash.Hash32 //记录了要存的crc，每次encode record的时候都会带上上一个文件的crc(上一个文件的crc就是存在这里)（如果是文件的第一条crcType的话）
	// 如果不是第一条，crc就是存的data的校验值。每次拿出来用的时候，都要校验crc和用data生成的crc是否一致，不一致数据就被破坏了。
	// 非crcType类型record，看着只有文件的最后一个是有用的，这个文件的最后一个record的crc，就是下个文件crcType的那个crc。
	//这个buf太细了吧，开辟了一个1M的buf，用来避免重复的开辟空间，只要小于1M的record消息，都是直接Marshal到buf中再write。
	//如果大于1M，会重新开辟内存去write，因为record大部分都是小于1M的，所以这个buf减少了很多次内存开辟啊。666
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

	e.crc.Write(rec.Data) // 这个crc是自己封装的，里面有一个crc成员，Write就是把算出来的校验码存在crc成员中了。
	// todo 这无论如何都把rec.Crc给改了，那如果encode的是CrcType，那传的prevCrc有用？
	// todo 懂了，在encode之前都NewFileEncoder了，这时候传进来的crc已经存到e.crc中了，然后当是crcType类型的时候，rec.Data==nil，直接返回了e.crc，
	// todo 所以传进来的是没用的，是用的newFileEncoder传的，这俩传的又都是一样的。相当于条记录都会有crc。
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
