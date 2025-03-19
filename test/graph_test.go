package test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	hnswindex "github.com/bmeg/hnsw-index"
	"github.com/bmeg/hnsw-index/distqueue"
	"github.com/cockroachdb/pebble"
)

func RandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	characters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	str := make([]rune, length)
	for i := range str {
		str[i] = characters[rand.Intn(len(characters))]
	}
	return string(str)
}

func Test_Insert(t *testing.T) {
	dbname := "test_index." + RandomString(5)
	idx, err := hnswindex.New(dbname)
	if err != nil {
		t.Error(err)
	}

	//params
	dim := 256
	var layers uint8 = 8
	var mMax uint8 = 10
	var mMax0 uint8 = 20
	var efCount int = 20
	var epUpdateFreq uint32 = 500

	g, err := idx.NewGraph("graph1", dim, layers, mMax, mMax0, efCount, epUpdateFreq)
	if err != nil {
		t.Error(err)
	}

	const numVecs = 2000
	startGen := time.Now()
	vmap := make(map[string][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		c := make([]float32, dim)
		for j := 0; j < dim; j++ {
			c[j] = rand.Float32()
		}
		vmap[fmt.Sprintf("vec_%d", i)] = c
	}
	fmt.Printf("Generated 3000 vectors in %v\n", time.Since(startGen))

	fmt.Println("Inserting vectors...")
	batch := idx.Db.NewBatch()
	startInsert := time.Now()
	for k, v := range vmap {
		err := g.Insert([]byte(k), v, batch)
		if err != nil {
			t.Error(err)
		}
		if batch.Len() > numVecs/10 {
			if err := batch.Commit(pebble.Sync); err != nil {
				t.Error(err)
			}
			batch.Close()
			batch = idx.Db.NewBatch()
		}
	}
	if batch.Len() > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			t.Error(err)
		}
		batch.Close()
	}
	insertTime := time.Since(startInsert)
	fmt.Printf("Inserted %d vectors in %v (%.2f vectors/sec)\n", numVecs, insertTime, float64(numVecs)/insertTime.Seconds())

	// Print layer stats
	for l := uint8(0); l <= layers; l++ {
		edgeCount := 0
		nodes := make(map[uint64]bool)
		for e := range g.ListLayer(l) {
			edgeCount++
			nodes[e.Source] = true
			nodes[e.Dest] = true
		}
		fmt.Printf("Layer %d: %d edges, %d nodes\n", l, edgeCount, len(nodes))
	}

	// Test 10 queries
	numQueries := 10
	K := 10
	efValues := []int{10, 20, 50, 100}
	recallSum := 0.0

	for q := 0; q < numQueries; q++ {
		qName := fmt.Sprintf("vec_%d", rand.Intn(numVecs))
		qVec := vmap[qName]

		// Brute-force ground truth
		startBrute := time.Now()
		testDists := distqueue.NewMin[float32, string]()
		for k, v := range vmap {
			d := hnswindex.Euclidean(v, qVec)
			testDists.Insert(d, k)
		}
		bruteTime := time.Since(startBrute)

		// HNSW search with varying ef
		for _, ef := range efValues {
			startSearch := time.Now()
			out, err := g.Search(qVec, K, ef)
			if err != nil {
				t.Error(err)
			}
			searchTime := time.Since(startSearch)

			trueNeighbors := make(map[string]bool)
			for i := 0; i < K && i < len(testDists); i++ {
				trueNeighbors[testDists[i].Value] = true
			}
			hits := 0
			for _, i := range out {
				if trueNeighbors[string(i)] {
					hits++
				}
			}
			recall := float64(hits) / float64(K)
			recallSum += recall
			fmt.Printf("Query %d, ef=%d: HNSW found %d/%d true neighbors, Recall=%.3f, Search time=%v\n",
				q, ef, hits, K, recall, searchTime)
			if recall < 0.9 {
				fmt.Printf("HNSW results (ef=%d): %v\n", ef, out)
				fmt.Printf("Brute-force top 10: ")
				for i := 0; i < K && i < len(testDists); i++ {
					fmt.Printf("%s (%.3f) ", testDists[i].Value, testDists[i].Dist)
				}
				fmt.Println()
			}
		}
		fmt.Printf("Brute-force time for Query %d: %v\n", q, bruteTime)
	}

	avgRecall := recallSum / float64(numQueries*len(efValues))
	fmt.Printf("Average Recall across %d queries: %.3f\n", numQueries, avgRecall)

	idx.Close()
	os.RemoveAll(dbname)
}

func GenerateBioVectors(numVectors, dim, numClusters int) map[string][]float32 {
	vmap := make(map[string][]float32, numVectors)
	rand.Seed(time.Now().UnixNano())
	centers := make([][]float32, numClusters)
	// Generate cluster centers (e.g., distinct biological subtypes)
	for c := 0; c < numClusters; c++ {
		center := make([]float32, dim)
		for j := 0; j < dim; j++ {
			center[j] = rand.Float32() * 100 // Larger range for biological variability
		}
		centers[c] = center
	}
	// Generate vectors with noise around centers
	for i := 0; i < numVectors; i++ {
		c := make([]float32, dim)
		cluster := rand.Intn(numClusters)
		for j := 0; j < dim; j++ {
			// Gaussian noise to simulate biological variation
			noise := float32(rand.NormFloat64()) * 0.5
			c[j] = centers[cluster][j] + noise
		}
		vmap[fmt.Sprintf("%d", i)] = c
	}
	return vmap
}

func Test_BioScale(t *testing.T) {
	dbname := "bio_index." + RandomString(5)
	idx, err := hnswindex.New(dbname)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	defer os.RemoveAll(dbname)

	// Params
	numVectors := 2000
	dim := 512
	numClusters := 10
	var layers uint8 = 5
	var mMax uint8 = 10
	var mMax0 uint8 = 10
	var efCount int = 20
	var epUpdateFreq uint32 = 500

	g, err := idx.NewGraph("bio_graph", dim, layers, mMax, mMax0, efCount, epUpdateFreq)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Generating vectors...")
	start := time.Now()
	vmap := GenerateBioVectors(numVectors, dim, numClusters)
	fmt.Printf("Generated %d vectors in %v\n", numVectors, time.Since(start))

	// Insert vectors
	fmt.Println("Inserting vectors...")
	start = time.Now()
	batch := idx.Db.NewBatch()
	for k, v := range vmap {
		err := g.Insert([]byte(k), v, batch)
		if err != nil {
			t.Fatal(err)
		}
		if batch.Len() > 300 { // Commit every 10K writes
			batch.Commit(nil)
			batch.Close()
			batch = idx.Db.NewBatch()
		}
	}
	insertTime := time.Since(start)
	fmt.Printf("Inserted %d vectors in %v (%.2f vectors/sec)\n", numVectors, insertTime, float64(numVectors)/insertTime.Seconds())

	// Log graph structure
	for l := uint8(0); l <= layers; l++ {
		count := 0
		nodes := make(map[uint64]bool)
		for e := range g.ListLayer(l) {
			nodes[e.Source] = true
			nodes[e.Dest] = true
			count++
		}
		fmt.Printf("Layer %d: %d edges, %d nodes\n", l, count, len(nodes))
	}

	// Test queries
	numQueries := 10
	K := 10
	efValues := []int{10, 20, 50, 100}
	recallSum := 0.0

	for q := 0; q < numQueries; q++ {
		qName := fmt.Sprintf("%d", rand.Intn(numVectors))
		qVec := vmap[qName]

		// Brute-force ground truth (optional for large scale, can sample)
		start = time.Now()
		testDists := distqueue.NewMin[float32, string]()
		for k, v := range vmap {
			d := hnswindex.Euclidean(v, qVec)
			testDists.Insert(d, k)
		}
		bruteTime := time.Since(start)

		for _, ef := range efValues {
			start = time.Now()
			out, err := g.Search(qVec, K, ef)
			if err != nil {
				t.Fatal(err)
			}
			searchTime := time.Since(start)

			// Compute recall
			trueNeighbors := make(map[string]bool)
			for i := 0; i < K && i < len(testDists); i++ {
				trueNeighbors[testDists[i].Value] = true
			}
			hits := 0
			for _, i := range out {
				if trueNeighbors[string(i)] {
					hits++
				}
			}
			recall := float64(hits) / float64(K)
			recallSum += recall
			fmt.Printf("Query %d, ef=%d: HNSW found %d/%d true neighbors, Recall=%.3f, Search time=%v\n",
				q, ef, hits, K, recall, searchTime)
			if recall < 0.8 {
				fmt.Printf("HNSW results (ef=%d): %v\n", ef, out)
				fmt.Printf("Brute-force top %d: ", K)
				for i := 0; i < K; i++ {
					fmt.Printf("%s (%.3f) ", testDists[i].Value, testDists[i].Dist)
				}
				fmt.Println()
			}
		}
		fmt.Printf("Brute-force time for Query %d: %v\n", q, bruteTime)
	}

	avgRecall := recallSum / float64(numQueries*len(efValues))
	fmt.Printf("Average Recall across %d queries: %.3f\n", numQueries, avgRecall)
}
