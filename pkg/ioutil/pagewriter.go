// Copyright 2016 The etcd Authors
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

package ioutil

import (
	"io"
)

var defaultBufferBytes = 128 * 1024

// PageWriter implements the io.Writer interface so that writes will
// either be in page chunks or from flushing.
// PageWriter实现io.Writer接口，以便写入可以在页面块中进行，也可以从刷新中进行。
type PageWriter struct {
	w io.Writer
	// pageOffset tracks the page offset of the base of the buffer
	// pageOffset跟踪缓冲区底部的页面偏移量
	pageOffset int
	// pageBytes is the number of bytes per page
	// pageBytes是每页的字节数
	pageBytes int

	// bufferedBytes counts the number of bytes pending for write in the buffer
	// bufferedBytes 表示 buf 中已经存入的数据的长度
	bufferedBytes int
	// buf holds the write buffer
	buf []byte
	// bufWatermarkBytes is the number of bytes the buffer can hold before it needs
	// to be flushed. It is less than len(buf) so there is space for slack writes
	// to bring the writer to page alignment.
	bufWatermarkBytes int
}

// NewPageWriter creates a new PageWriter. pageBytes is the number of bytes
// to write per page. pageOffset is the starting offset of io.Writer.
// NewPageWriter创建一个新的PageWriter。 pageBytes是每页要写入的字节数。 pageOffset是io.Writer的起始偏移量。
func NewPageWriter(w io.Writer, pageBytes, pageOffset int) *PageWriter {
	return &PageWriter{
		w:                 w,
		pageOffset:        pageOffset,
		pageBytes:         pageBytes,                                  // 4k
		buf:               make([]byte, defaultBufferBytes+pageBytes), // 4k+128k 为啥要多4k
		bufWatermarkBytes: defaultBufferBytes,                         // 128k
	}
}

func (pw *PageWriter) Write(p []byte) (n int, err error) {
	// pw.bufferedBytes 一开始是0 ，所以第一次判断是判断len(p) 是否不超过 pw.bufWatermarkBytes (128k)
	if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
		// no overflow 不超过128k ，将p中的数据放入buf
		copy(pw.buf[pw.bufferedBytes:], p)
		//pw.bufferedBytes 增加相应buf存入的数据长度
		pw.bufferedBytes += len(p)
		return len(p), nil
	}
	// complete the slack page in the buffer if unaligned
	// 如果未对齐，请完成缓冲区中的空闲页， slack代表空闲区，需要补充。
	slack := pw.pageBytes - ((pw.pageOffset + pw.bufferedBytes) % pw.pageBytes)
	if slack != pw.pageBytes { //如果有空闲区
		partial := slack > len(p)
		if partial {
			// not enough data to complete the slack page
			slack = len(p)
		}
		// special case: writing to slack page in buffer
		copy(pw.buf[pw.bufferedBytes:], p[:slack]) //这就会有可能用到那多余的4k。比如上一次填入bufferedBytes正好128k，这次就会走到这里来。
		pw.bufferedBytes += slack                  //这时候 pw.bufferedBytes就可能超过128k
		n = slack
		p = p[slack:]
		if partial {
			// avoid forcing an unaligned flush 避免强行执行一个未定义的flush
			return n, nil
		}
	}
	// buffer contents are now page-aligned; clear out
	// 这时候将buf中的数据刷入磁盘，因为buf中的内容正好是页面对其的。就4k对其。
	if err = pw.Flush(); err != nil {
		return n, err
	}
	// directly write all complete pages without copying 直接写所有完整的页面而无需复制
	if len(p) > pw.pageBytes { //如果目前的p仍然大于4k（一页的量）
		pages := len(p) / pw.pageBytes
		c, werr := pw.w.Write(p[:pages*pw.pageBytes]) //将整数页写入磁盘
		n += c
		if werr != nil {
			return n, werr
		}
		p = p[pages*pw.pageBytes:]
	}
	// write remaining tail to buffer
	c, werr := pw.Write(p) //最后将剩余的写入磁盘（末尾不足一页的部分。） 为啥要调用自己？因为要存到缓存中，不flush
	n += c
	return n, werr
}

// Flush flushes buffered data.
func (pw *PageWriter) Flush() error {
	_, err := pw.flush()
	return err
}

// FlushN flushes buffered data and returns the number of written bytes.
func (pw *PageWriter) FlushN() (int, error) {
	return pw.flush()
}

func (pw *PageWriter) flush() (int, error) {
	if pw.bufferedBytes == 0 {
		return 0, nil
	}
	n, err := pw.w.Write(pw.buf[:pw.bufferedBytes])
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageBytes
	pw.bufferedBytes = 0
	return n, err
}
