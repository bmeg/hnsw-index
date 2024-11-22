package test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	hnswindex "github.com/bmeg/hnsw-index"
	"github.com/bmeg/hnsw-index/distqueue"
)

func TestInsert(t *testing.T) {

	dbname := "test_index." + RandomString(5)

	idx, err := hnswindex.New(dbname)

	if err != nil {
		t.Error(err)
	}

	dim := 50
	layers := 5

	g, err := idx.NewGraph("graph1", dim, uint8(layers), 10)
	if err != nil {
		t.Error(err)
	}

	vmap := map[string][]float32{}
	for i := 0; i < 100; i++ {
		c := make([]float32, dim)
		for j := 0; j < dim; j++ {
			c[j] = rand.Float32()
		}
		vmap[fmt.Sprintf("%d", i)] = c
	}

	for k, v := range vmap {
		//fmt.Printf("==vector==:%s\n", k)
		err := g.Insert([]byte(k), v)
		if err != nil {
			t.Error(err)
		}
	}

	/*
		fmt.Printf("===Layer 0===\n")
		for e := range g.ListLayer(0) {
			fmt.Printf("%d %d %f\n", e.Source, e.Dest, e.Dist)
		}

		fmt.Printf("===Layer 1==\n")
		for e := range g.ListLayer(1) {
			fmt.Printf("%d %d %f\n", e.Source, e.Dest, e.Dist)
		}

		fmt.Printf("===Layer 2==\n")
		for e := range g.ListLayer(2) {
			fmt.Printf("%d %d %f\n", e.Source, e.Dest, e.Dist)
		}
	*/

	qName := "10"
	qVec := vmap[qName]
	testDists := distqueue.NewMin[float32, string]()

	for k, v := range vmap {
		d := hnswindex.Euclidean(v, qVec)
		testDists.Insert(d, k)
	}

	out, err := g.Search(vmap[qName], 10, 20)
	if err != nil {
		t.Error(err)
	}

	for _, i := range out {
		fmt.Printf("search: out: %s\n", i)
	}

	for i := 0; i < 10; i++ {
		fmt.Printf("scan out: %s\n", testDists[i].Value)
	}

	idx.Close()
	os.RemoveAll(dbname)

}
