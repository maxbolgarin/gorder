package gorder

import (
	"sync"
)

// LinkedList is an implementation of a thread-safe generic doubly linked list.
type LinkedList[T any] struct {
	head *node[T]
	tail *node[T]
	len  int
	mu   sync.Mutex
}

type node[T any] struct {
	prev *node[T]
	next *node[T]
	data T
}

// NewLinkedList creates a new linked list.
func NewLinkedList[T any]() *LinkedList[T] {
	return &LinkedList[T]{}
}

// Front returns the first element of the linked list.
func (l *LinkedList[T]) Front() (T, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head == nil {
		var zero T
		return zero, false
	}
	return l.head.data, true
}

// Back returns the last element of the linked list.
func (l *LinkedList[T]) Back() (T, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		var zero T
		return zero, false
	}
	return l.tail.data, true
}

// Len returns the number of elements in the linked list.
func (l *LinkedList[T]) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.len
}

// InsertFront adds an element to the front of the linked list.
func (l *LinkedList[T]) InsertFront(data T) {
	l.insert(data, func(l *LinkedList[T], newNode *node[T]) {
		if l.head != nil {
			l.head.next = newNode
			newNode.prev = l.head
			l.head = newNode
		}
	})
}

// InsertBack adds an element to the back of the linked list.
func (l *LinkedList[T]) InsertBack(data T) {
	l.insert(data, func(l *LinkedList[T], newNode *node[T]) {
		if l.tail != nil {
			l.tail.prev = newNode
			newNode.next = l.tail
			l.tail = newNode
		}
	})
}

// PopFront removes an element from the front of the linked list and returns it.
func (l *LinkedList[T]) PopFront() (T, bool) {
	return l.pop(l.head, func(l *LinkedList[T]) {
		l.head = l.head.prev
		if l.head != nil {
			l.head.next = nil
		}
	})
}

// PopBack removes an element from the back of the linked list and returns it.
func (l *LinkedList[T]) PopBack() (T, bool) {
	return l.pop(l.tail, func(l *LinkedList[T]) {
		l.tail = l.tail.next
		if l.tail != nil {
			l.tail.prev = nil
		}
	})
}

func (l *LinkedList[T]) insert(data T, inserter func(l *LinkedList[T], newNode *node[T])) {
	l.mu.Lock()
	defer l.mu.Unlock()
	defer func() {
		l.len += 1
	}()

	newNode := &node[T]{
		data: data,
	}

	if l.len == 0 {
		l.head = newNode
		l.tail = newNode
		return
	}

	inserter(l, newNode)
}

func (l *LinkedList[T]) pop(node *node[T], popper func(l *LinkedList[T])) (T, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if node == nil {
		var zero T
		return zero, false
	}
	out := node.data

	l.len -= 1
	if l.len == 0 {
		l.head = nil
		l.tail = nil
		return out, true
	}

	popper(l)
	return out, true
}
