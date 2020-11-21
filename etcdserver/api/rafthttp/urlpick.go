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
	"net/url"
	"sync"

	"go.etcd.io/etcd/v3/pkg/types"
)

//就是一个url.URL的slice,然后每次通过它peek的时候都默认拿第一个url，如果第一个url有问题，
//可以调用unreachable函数来以轮询的方式移动picked，然后选出下一个url。
// 基本调用逻辑 newURLPicker() -> pick()  -> unreachable() -> pick()
// newURLPicker() 就是初始化一个该结构
//  pick() 就是取一个url，picked就是索引，取该索引的 url
// 如果pick()取出来的url我们用这有问题，可以调用unreachable()来轮询picked
// 然后在pick()出另一个url
type urlPicker struct {
	mu     sync.Mutex // guards urls and picked
	urls   types.URLs // 一个url.URL的slice
	picked int
}

func newURLPicker(urls types.URLs) *urlPicker {
	return &urlPicker{
		urls: urls,
	}
}

func (p *urlPicker) update(urls types.URLs) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.urls = urls
	p.picked = 0
}

func (p *urlPicker) pick() url.URL {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.urls[p.picked]
}

// unreachable notices the picker that the given url is unreachable,
// and it should use other possible urls.
// unreachable通知选择器给定的URL是不可达的，它应该使用其他可能的URL。
// 告知传入的url不能用，选择另外一个，选择规则是轮训
func (p *urlPicker) unreachable(u url.URL) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if u == p.urls[p.picked] {
		p.picked = (p.picked + 1) % len(p.urls)
	}
}
