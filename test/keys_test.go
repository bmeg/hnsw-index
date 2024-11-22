package test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	hnswindex "github.com/bmeg/hnsw-index"
	"github.com/cockroachdb/pebble"
)

// RandomString generates a random string of length n.
func RandomString(n int) string {
	rand.NewSource(int64(time.Now().UnixNano()))
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

func TestLayerOrder(t *testing.T) {

	dbname := "test." + RandomString(5)
	db, err := pebble.Open(dbname, &pebble.Options{})
	if err != nil {
		t.Error(err)
	}

	data := map[uint64][]float32{
		1:    {13.0, 16, 23.0, 67.0, 3000},
		2:    {1.0, 73, 1023.0, 6.0},
		4048: {1023.0, 1.0, 6.0, 73},
	}

	for k, vs := range data {
		for _, v := range vs {
			key := hnswindex.LayerKeyEncode(1, 2, k, v)
			db.Set(key, []byte{}, nil)
		}
	}

	iter, err := db.NewIter(nil)
	if err != nil {
		t.Error(err)
	}

	curSource := uint64(0)
	curDist := float32(0.0)
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		graphID, layer, source, dist := hnswindex.LayerKeyParse(iter.Key())
		if layer != 2 {
			t.Errorf("Layer not coded correctly: %d %d", 2, layer)

		}
		if curSource > source {
			t.Errorf("Sources out of order: %d %d", curSource, source)
		}
		if curSource != source {
			curDist = 0.0
		}
		if curDist > dist {
			t.Errorf("Distances out of order: %f %f", curDist, dist)
		}
		curDist = dist
		curSource = source
		fmt.Printf("%d %d %f\n", graphID, source, dist)
		count++
	}
	fmt.Printf("Keys found: %d\n", count)

	db.Close()
	os.RemoveAll(dbname)
}

func TestVecEncoder(t *testing.T) {

	vec := []float32{1.0, 2.0, 3.0, 4.0, 10.0, 11.0}
	val := hnswindex.VectorValueEncode(vec)
	newVec := hnswindex.VectorValueParse(val)

	for i := range vec {
		if vec[i] != newVec[i] {
			t.Errorf("mismtach value: %f %f", vec[i], newVec[i])
		}
	}
}
