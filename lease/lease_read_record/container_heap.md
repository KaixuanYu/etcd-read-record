# Package heap

`import "container/heap"`

[toc]

## Overview 概述

Package heap provides heap operations for any type that implements heap.Interface. A heap is a tree with the property that each node is the minimum-valued node in its subtree.

`heap` 包为所有实现了 `heap.Interface` 的类型提供 `heap` 操作。 heap(堆)是一棵树，其属性为每个节点是其子树中的最小值节点。

The minimum element in the tree is the root, at index 0.

树中的最小元素是根，索引为0。

A heap is a common way to implement a priority queue. To build a priority queue, implement the Heap interface with the (negative) priority as the ordering for the Less method, so Push adds items while Pop removes the highest-priority item from the queue. The Examples include such an implementation; the file example_pq_test.go has the complete source.

堆是实现优先级队列的常用方法。 要构建优先级队列，请以（负）优先级实现Heap接口作为Less方法的顺序，因此Push添加项，而Pop删除队列中优先级最高的项。 示例包括这样的实现； 文件example_pq_test.go

### int head 示例
本示例将多个int插入IntHeap中，检查最小值，然后按优先级顺序将其删除。
```
// This example demonstrates an integer heap built using the heap interface.
//本示例演示使用堆接口构建的整数堆。
package main

import (
	"container/heap"
	"fmt"
)

// An IntHeap is a min-heap of ints.
//IntHeap是int的最小堆。
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
    // Push和Pop使用指针接收器，因为它们修改了切片的长度，而不仅仅是其内容。
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// This example inserts several ints into an IntHeap, checks the minimum,
// and removes them in order of priority.
// 本示例将多个int插入IntHeap中，检查最小值，然后按优先级顺序将其删除。
func main() {
	h := &IntHeap{2, 1, 5}
	heap.Init(h)
	heap.Push(h, 3) // 将3放进堆中。
	fmt.Printf("minimum: %d\n", (*h)[0]) // 输出1
	for h.Len() > 0 {
		fmt.Printf("%d ", heap.Pop(h))   // 输出 1 2 3 5
	}
}
```

### PriorityQueue 优先队列实例
本示例创建一个包含某些项目的PriorityQueue，添加和操作一个项目，然后按优先级顺序删除这些项目。

```
// This example demonstrates a priority queue built using the heap interface.
//此示例演示了使用堆接口构建的优先级队列。
package main

import (
	"container/heap"
	"fmt"
)

// An Item is something we manage in a priority queue.
//项目是我们在优先级队列中管理的项目。
type Item struct {
	value    string // The value of the item; arbitrary.
	priority int    // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
    // 索引是更新所需的，并由heap.Interface方法维护。
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
// PriorityQueue 实现了 heap.Interface 接口并保存items
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
    // 我们希望Pop给我们最高而不是最低的优先级，因此我们使用的优先级高于此处。
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value string, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

// This example creates a PriorityQueue with some items, adds and manipulates an item,
// and then removes the items in priority order.
func main() {
	// Some items and their priorities.
	items := map[string]int{
		"banana": 3, "apple": 2, "pear": 4,
	}

	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := make(PriorityQueue, len(items))
	i := 0
	for value, priority := range items {
		pq[i] = &Item{
			value:    value,
			priority: priority,
			index:    i,
		}
		i++
	}
	heap.Init(&pq)

	// Insert a new item and then modify its priority.
	item := &Item{
		value:    "orange",
		priority: 1,
	}
	heap.Push(&pq, item)
	pq.update(item, item.value, 5)

	// Take the items out; they arrive in decreasing priority order.
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		fmt.Printf("%.2d:%s ", item.priority, item.value)
		// 返回 05:orange 04:pear 03:banana 02:apple
	}
}

```

## Index 索引
```
func Fix(h Interface, i int)
func Init(h Interface)
func Pop(h Interface) interface{}
func Push(h Interface, x interface{})
func Remove(h Interface, i int) interface{}
type Interface
```

### func Fix
`func Fix(h Interface, i int)`

Fix re-establishes the heap ordering after the element at index i has changed its value. Changing the value of the element at index i and then calling Fix is equivalent to, but less expensive than, calling Remove(h, i) followed by a Push of the new value. The complexity is O(log n) where n = h.Len().

Fix 在某个元素的值被改变后，重新建立 `heap` 的顺序。更改i的值然后调用Fix函数，相当于在Remove掉i之后，再Push新值，但是Fix的成本更低。复杂度为O(logn)，其中 n=h.Len()。

### func Init
`func Init(h Interface)`

Init establishes the heap invariants required by the other routines in this package. Init is idempotent with respect to the heap invariants and may be called whenever the heap invariants may have been invalidated. The complexity is O(n) where n = h.Len().

Init建立此包中其他例程所需的堆不变量。Init相对于堆不变量是幂等的，并且可以在堆不变量失效时调用。复杂度为O（n），其中n=h.Len（）。

### func Pop
`func Pop(h Interface) interface()`

Pop removes and returns the minimum element (according to Less) from the heap. The complexity is O(log n) where n = h.Len(). Pop is equivalent to Remove(h, 0).

Pop 删除并返回heap中最小的那个元素（基于Less函数的实现，如果less中认为大的元素更小，那么就是返回最大的元素了）。其复杂度是 O(log n) n=h.Len()。 Pop相当于 Remove(h, 0) 。就是相当于删除了第0号元素，并返回。

### func Push
`func Push(h Interface, x interface{})`

Push pushes the element x onto the heap. The complexity is O(log n) where n = h.Len().

Push 将元素放入堆中。复杂度 O(log n)

### func Remove
`func Remove(h Interface, i int) interface{}`

Remove removes and returns the element at index i from the heap. The complexity is O(log n) where n = h.Len().

Remove 删除并返回在 i 索引出的 元素。 复杂度是 O(log n)

### type Interface

The Interface type describes the requirements for a type using the routines in this package. Any type that implements it may be used as a min-heap with the following invariants (established after Init has been called or if the data is empty or sorted):

接口类型使用此包中的例程描述对类型的要求。 任何实现它的类型都可以用作带有以下不变量的最小堆（在调用Init或数据为空或排序后建立）。

`!h.Less(j, i) for 0 <= i < h.Len() and 2*i+1 <= j <= 2*i+2 and j < h.Len()`

Note that Push and Pop in this interface are for package heap's implementation to call. To add and remove things from the heap, use heap.Push and heap.Pop.

请注意，此接口中的Push和Pop用于包堆的实现调用。 要从堆中添加和删除内容，请使用heap.Push和heap.Pop。

```
type Interface interface {
    sort.Interface
    Push(x interface{}) // add x as element Len()
    Pop() interface{}   // remove and return element Len() - 1.
}
```