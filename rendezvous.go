package rendezvous

import (
	stdhash "hash"
	"hash/fnv"
	"io"
	"math"
	"sort"
	"sync"
)

const (
	defaultWeight = 1.0
)

// A Ring is a collection of nodes making up a rendezvous group.
// Nodes have a label and, optionally, a weight.  If unspecified,
// a default weighting is used.
type Ring struct {
	nodes []*Node
	hash  stdhash.Hash64
	mutex sync.RWMutex
}

type Node struct {
	name   string
	hash   uint64
	weight float64
}

type ScoredNode struct {
	node  *Node
	score float64
}

func New() *Ring {
	return NewWithHash(fnv.New64a())
}

func NewWithHash(hash stdhash.Hash64) *Ring {
	return &Ring{
		nodes: make([]*Node, 0),
		hash:  hash,
		mutex: sync.RWMutex{},
	}
}

func (r *Ring) Contains(name string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	for _, n := range r.nodes {
		if n.name == name {
			return true
		}
	}
	return false
}

func (r *Ring) Add(name string) {
	r.AddWithWeight(name, defaultWeight)
}

func (r *Ring) AddWithWeight(name string, weight float64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	ix := sort.Search(len(r.nodes), r.cmp(name))

	if ix < len(r.nodes) && r.nodes[ix].name == name {
		r.nodes[ix].weight = weight
	} else {
		n := &Node{
			name:   name,
			hash:   r.computeHash(name),
			weight: weight,
		}
		r.nodes = append(r.nodes, nil)
		copy(r.nodes[ix+1:], r.nodes[ix:])
		r.nodes[ix] = n
	}
}

func (r *Ring) Remove(name string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	ix := sort.Search(len(r.nodes), r.cmp(name))
	if ix == len(r.nodes) {
		return
	}

	if r.nodes[ix].name == name {
		copy(r.nodes[:ix], r.nodes[:ix+1])
		r.nodes = r.nodes[:len(r.nodes)-1]
	}
}

func (r *Ring) LookupAll(key string) []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	keyHash := r.computeHash(key)

	scoredNodes := make([]ScoredNode, 0)
	for _, node := range r.nodes {
		score := computeScore(keyHash, node.hash, node.weight)
		scoredNodes = append(scoredNodes, ScoredNode{node: node, score: score})
	}

	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].score > scoredNodes[j].score
	})

	names := make([]string, 0)
	for _, namedNode := range scoredNodes {
		names = append(names, namedNode.node.name)
	}

	return names
}

func (r *Ring) LookupTopN(key string, n int) []string {
	names := r.LookupAll(key)

	if len(names) >= n {
		return names[:n]
	}

	return names
}

func (r *Ring) Lookup(key string) string {
	names := r.LookupTopN(key, 1)
	if len(names) > 0 {
		return names[0]
	}
	return ""
}

func (r *Ring) Weight(name string) float64 {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	ix := sort.Search(len(r.nodes), r.cmp(name))
	if ix == len(r.nodes) {
		return 0
	}

	return r.nodes[ix].weight
}

func (r *Ring) List() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	ns := make([]string, 0)
	for _, n := range r.nodes {
		ns = append(ns, n.name)
	}
	return ns
}

func (r *Ring) Len() int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return len(r.nodes)
}

func (r *Ring) computeHash(name string) uint64 {
	r.hash.Reset()
	_, _ = io.WriteString(r.hash, name)
	return r.hash.Sum64()
}

func (r *Ring) cmp(name string) func(int) bool {
	return func(i int) bool {
		return r.nodes[i].name >= name
	}
}

func computeScore(keyHash, nodeHash uint64, nodeWeight float64) float64 {
	h := combineHashes(keyHash, nodeHash)
	return -nodeWeight / math.Log(float64(h)/float64(math.MaxUint64))
}

func combineHashes(a, b uint64) uint64 {
	// uses the "xorshift*" mix function which is simple and effective
	// see: https://en.wikipedia.org/wiki/Xorshift#xorshift*
	x := a ^ b
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	return x * 0x2545F4914F6CDD1D
}
