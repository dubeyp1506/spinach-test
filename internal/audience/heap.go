package audience

import (
	"container/heap"
	"sort"
)

// Bounded min-heap top-K selection — CONTRACTS §5 algorithm contract.
//
// Why a heap and not a sort: ranking N streamed rows by taking the K best
// with a full sort costs O(N log N) time and O(N) space — every row must be
// materialized before sorting. A bounded MIN-heap of capacity K costs
// O(N log K) time (an O(log K) sift-down only for candidates that beat the
// current K-th best; rejects are O(1)) and O(K) space.
// At demo scale (N≈1e5–1e6, K≈1e3) that is the difference between sorting a
// million rows and keeping a ~2 000-entry heap resident — see bench_test.go.
//
// MIN-heap on weighted score ⇒ the root is the weakest member of the current
// top set, so an incoming candidate only displaces it when strictly better.

// worse reports whether a ranks strictly below b. Order is weighted score
// DESC; ties break on customerID ASC so results are deterministic and match
// the reference sort used in tests.
func worse(a, b candidate) bool {
	if a.weighted != b.weighted {
		return a.weighted < b.weighted
	}
	return a.customerID > b.customerID
}

func better(a, b candidate) bool { return worse(b, a) }

// minHeap implements heap.Interface; element 0 is always the worst of the set.
type minHeap []candidate

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return worse(h[i], h[j]) }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x any) { *h = append(*h, x.(candidate)) }

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	*h = old[:n-1]
	return old[n-1]
}

// topKHeap keeps the best `cap` candidates seen so far.
type topKHeap struct {
	h   minHeap
	cap int
}

func newTopKHeap(capacity int) *topKHeap {
	return &topKHeap{h: make(minHeap, 0, max(capacity, 0)), cap: capacity}
}

// Add offers one candidate. O(log K) when it enters the heap, O(1) when it
// cannot beat the current worst member. Amortized over a stream of N rows:
// O(N log K) total.
func (t *topKHeap) Add(c candidate) {
	switch {
	case t.cap <= 0:
		return
	case len(t.h) < t.cap:
		heap.Push(&t.h, c) // O(log K)
	case worse(t.h[0], c):
		// Root is the weakest of the top set: overwrite + sift down is one
		// O(log K) fix, cheaper than Pop+Push (also O(log K) but two ops).
		t.h[0] = c
		heap.Fix(&t.h, 0)
	}
}

// Sorted returns the heap contents in ranking order (best first). O(K log K)
// on at most K elements — this is NOT the O(N log N) sort the heap avoids;
// the N-row stream was already reduced to ≤K survivors.
func (t *topKHeap) Sorted() []candidate {
	out := make([]candidate, len(t.h))
	copy(out, t.h)
	sort.Slice(out, func(i, j int) bool { return better(out[i], out[j]) })
	return out
}
