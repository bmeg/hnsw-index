package test

import (
	"fmt"
	"testing"

	"github.com/bmeg/hnsw-index/distqueue"
)

func TestDistMinCappedQueue(t *testing.T) {
	d := distqueue.NewMinCapped[float32, int](5)

	d.Insert(2.0, 2)
	d.Insert(9.0, 9)
	d.Insert(10.0, 10)
	d.Insert(6.0, 6)
	d.Insert(7.0, 7)
	d.Insert(1.0, 1)
	d.Insert(3.0, 3)
	d.Insert(4.0, 4)
	d.Insert(5.0, 5)
	d.Insert(8.0, 8)

	for _, i := range d {
		fmt.Printf("%f %d\n", i.Dist, i.Value)
	}
}

func TestDistMaxQueue(t *testing.T) {
	d := distqueue.NewMax[float32, int]()

	d.Insert(2.0, 2)
	d.Insert(9.0, 9)
	d.Insert(10.0, 10)
	d.Insert(6.0, 6)
	d.Insert(7.0, 7)
	d.Insert(1.0, 1)
	d.Insert(3.0, 3)
	d.Insert(4.0, 4)
	d.Insert(5.0, 5)
	d.Insert(8.0, 8)

	for _, i := range d {
		fmt.Printf("%f %d\n", i.Dist, i.Value)
	}
}
