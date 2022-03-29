package internal

import (
	"hash/fnv"
	"sort"
)

type KeyValue struct {
	Key   string
	Value string
}

// ByKey is used for sorting by key.
type ByKey []KeyValue

var _ sort.Interface = (*ByKey)(nil)

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
