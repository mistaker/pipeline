package pipeline

import (
	"sort"
	"testing"
)

func TestPipe(t *testing.T) {

	num := 100
	s := make([]int, 0, num)
	for i := 0; i < num; i++ {
		s = append(s, i)
	}

	t.Log(s)

	PipeLine(func(write writer) {
		for _, item := range s {
			write.write(item)
		}
	}, func(item interface{}, data chan<- interface{}) {
		echo := item.(int)
		echo++
		data <- echo
	}, func(data <-chan interface{}) {
		s := make([]int, 0, num)
		for item := range data {
			s = append(s, item.(int))
		}
		sort.SliceStable(s, func(i, j int) bool {
			return s[i] < s[j]
		})
		t.Log(s)
	})

}
