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
	}
}

```