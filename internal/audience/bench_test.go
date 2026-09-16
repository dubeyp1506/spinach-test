package audience

import (
	"math/rand"
	"sort"
	"testing"
)

// Benchmark artifact — CONTRACTS §5 requires the heap path documented against
// a full sort. N = 1e6, K = 1000.
//
// Measured on Apple M5 (`go test -bench=. -benchmem ./internal/audience/`):
//
//	BenchmarkTopKHeap-10   9,304,521 ns/op     341,592 B/op   1,006 allocs/op
//	BenchmarkFullSort-10 237,920,667 ns/op 112,002,275 B/op       5 allocs/op
//
// ~25x faster and ~330x less memory: the heap never materializes or sorts the
// full candidate set — O(N log K) time, O(K) space.
const (
	benchN = 1_000_000
	benchK = 1_000
)

var benchData = func() []candidate {
	rng := rand.New(rand.NewSource(7))
	d := make([]candidate, benchN)
	for i := range d {
		d[i] = candidate{customerID: int64(i + 1), weighted: rng.Float64() * 1e6}
	}
	return d
}()

func BenchmarkTopKHeap(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		th := newTopKHeap(benchK)
		for _, c := range benchData {
			th.Add(c)
		}
		_ = th.Sorted()
	}
}

func BenchmarkFullSort(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := append([]candidate(nil), benchData...)
		sort.Slice(s, func(i, j int) bool { return better(s[i], s[j]) })
		_ = s[:benchK]
	}
}
