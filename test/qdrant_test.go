package main_test

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- CONFIGURATION CONSTANTS ---
const (
	QdrantHost     = "127.0.0.1"
	QdrantPort     = 6334 // gRPC port
	CollectionName = "bio_benchmark_collection"
	NumVectors     = 40000
	Dim            = 512
	NumQueries     = 5
	K              = 100 // Top K neighbors to fetch
)

var efValues = []int{10, 20, 50, 100}

// Define a struct to hold aggregated results for comparison
type BenchmarkResult struct {
	HnswEf          int
	TotalRecall     float64
	TotalSearchTime time.Duration
	Count           int
}

// --- HELPER FUNCTIONS ---

// ptr creates a pointer to any value, replacing missing qdrant.New* helpers.
func ptr[T any](v T) *T {
	return &v
}

// Vector generation and distance functions remain the same...

func GenerateBioVectors(numVectors, dim, numClusters int) map[string][]float32 {
	// ... (Vector generation logic)
	rand.Seed(time.Now().UnixNano())
	vmap := make(map[string][]float32, numVectors)
	centers := make([][]float32, numClusters)
	for c := 0; c < numClusters; c++ {
		center := make([]float32, dim)
		for j := 0; j < dim; j++ {
			center[j] = rand.Float32() * 100
		}
		centers[c] = center
	}
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		cluster := rand.Intn(numClusters)
		for j := 0; j < dim; j++ {
			noise := float32(rand.NormFloat64()) * 0.5
			vec[j] = centers[cluster][j] + noise
		}
		vmap[fmt.Sprintf("%d", i)] = vec
	}
	return vmap
}

func Euclidean(v1, v2 []float32) float32 {
	var sum float64
	for i := range v1 {
		diff := float64(v1[i] - v2[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

type Neighbor struct {
	ID   string
	Dist float32
}

func getBruteForceNeighbors(queryVec []float32, vmap map[string][]float32, K int) []string {
	// ... (Brute force calculation logic)
	allDists := make([]Neighbor, 0, len(vmap))
	for id, vec := range vmap {
		dist := Euclidean(vec, queryVec)
		allDists = append(allDists, Neighbor{ID: id, Dist: dist})
	}
	for i := range allDists {
		for j := i + 1; j < len(allDists); j++ {
			if allDists[i].Dist > allDists[j].Dist {
				allDists[i], allDists[j] = allDists[j], allDists[i]
			}
		}
	}
	neighbors := make([]string, 0, K)
	for i := 0; i < K && i < len(allDists); i++ {
		neighbors = append(neighbors, allDists[i].ID)
	}
	return neighbors
}

func computeRecall(trueNeighbors []string, qdrantResults []string, K int) float64 {
	trueSet := make(map[string]struct{})
	for _, id := range trueNeighbors {
		trueSet[id] = struct{}{}
	}
	hits := 0
	for _, id := range qdrantResults {
		if _, found := trueSet[id]; found {
			hits++
		}
	}
	return float64(hits) / float64(K)
}

// formatVector converts []float32 to []float64 (required for upserting)
func formatVector(v []float32) []float64 {
	out := make([]float64, len(v))
	for i, val := range v {
		out[i] = float64(val)
	}
	return out
}

// ---------------------- MAIN TEST FUNCTION ----------------------

func TestQdrantBioBenchmark(t *testing.T) {
	NumClusters := 10

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", QdrantHost, QdrantPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Could not connect to Qdrant. Make sure port-forward is running on %s:%d: %v", QdrantHost, QdrantPort, err)
	}
	defer conn.Close()

	collectionsClient := qdrant.NewCollectionsClient(conn)
	pointsClient := qdrant.NewPointsClient(conn)

	// Clean up collection if it exists
	_, _ = collectionsClient.Delete(ctx, &qdrant.DeleteCollection{CollectionName: CollectionName})

	// 1. Create Collection
	fmt.Println("Creating collection...")
	vectorsConfig := &qdrant.VectorsConfig{
		Config: &qdrant.VectorsConfig_Params{
			Params: &qdrant.VectorParams{
				Size:     Dim,
				Distance: qdrant.Distance_Euclid,
			},
		},
	}
	_, err = collectionsClient.Create(ctx, &qdrant.CreateCollection{
		CollectionName: CollectionName,
		VectorsConfig:  vectorsConfig,
		Timeout:        ptr[uint64](10000),
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	fmt.Printf("Collection '%s' created (Dim=%d, Distance=Euclidean)\n", CollectionName, Dim)

	// 2. Generate Vectors and Insert Data
	fmt.Println("Generating and inserting vectors...")
	vmap := GenerateBioVectors(NumVectors, Dim, NumClusters)

	points := make([]*qdrant.PointStruct, 0, NumVectors)
	for idStr, v := range vmap {
		id, _ := strconv.ParseUint(idStr, 10, 64)
		points = append(points, &qdrant.PointStruct{
			Id: &qdrant.PointId{PointIdOptions: &qdrant.PointId_Num{Num: id}},
			// Use the NewVectors helper for single vector in PointStruct
			Vectors: qdrant.NewVectors(v...),
		})
	}

	start := time.Now()
	_, err = pointsClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: CollectionName,
		Wait:           ptr(true),
		Points:         points,
	})
	if err != nil {
		t.Fatalf("Failed to upsert points: %v", err)
	}
	insertTime := time.Since(start)
	fmt.Printf("Inserted %d vectors in %v (Index build complete)\n", NumVectors, insertTime)

	// 3. Run Benchmark Queries and Aggregate Results
	fmt.Println("\nStarting HNSW Recall Benchmark...")

	// Data structure to hold aggregated results per HnswEf value
	aggregatedResults := make(map[int]*BenchmarkResult)
	for _, ef := range efValues {
		aggregatedResults[ef] = &BenchmarkResult{HnswEf: ef}
	}

	var totalBruteTime time.Duration

	for q := 0; q < NumQueries; q++ {
		qID := fmt.Sprintf("%d", rand.Intn(NumVectors))
		qVec := vmap[qID] // []float32

		// Brute-Force Time Calculation (Ground Truth)
		start = time.Now()
		trueNeighbors := getBruteForceNeighbors(qVec, vmap, K)
		bruteTime := time.Since(start)
		totalBruteTime += bruteTime // Accumulate total brute-force time

		for _, ef := range efValues {
			searchParams := &qdrant.SearchParams{
				HnswEf: ptr[uint64](uint64(ef)),
			}

			// Execute Qdrant Search
			start = time.Now()
			searchResult, err := pointsClient.Search(ctx, &qdrant.SearchPoints{
				CollectionName: CollectionName,
				Vector:         qVec, // []float32 is correct for the Search field
				Limit:          uint64(K),
				Params:         searchParams,
				WithVectors:    qdrant.NewWithVectors(false),
				WithPayload:    qdrant.NewWithPayload(false),
			})
			if err != nil {
				log.Fatalf("Error during Search (Query %d, EF %d): %v", q+1, ef, err)
			}
			searchTime := time.Since(start)

			qdrantResults := make([]string, 0, len(searchResult.GetResult()))
			for _, hit := range searchResult.GetResult() {
				if idNum := hit.GetId().GetNum(); idNum != 0 {
					qdrantResults = append(qdrantResults, strconv.FormatUint(idNum, 10))
				}
			}

			recall := computeRecall(trueNeighbors, qdrantResults, K)

			// Aggregate results
			res := aggregatedResults[ef]
			res.TotalRecall += recall
			res.TotalSearchTime += searchTime
			res.Count++
		}
	}

	// 4. Final Comparison Report
	avgBruteTime := time.Duration(0)
	if NumQueries > 0 {
		avgBruteTime = totalBruteTime / time.Duration(NumQueries)
	}

	fmt.Printf("\n--- BENCHMARK COMPLETE ---\n")
	fmt.Printf("Total Queries Run: %d\n", NumQueries)
	fmt.Printf("Brute-Force Baseline (Avg. Search Time): %v\n", avgBruteTime)

	fmt.Printf("\n--- QDRANT HNSW PERFORMANCE VS. BRUTE-FORCE ---\n")
	fmt.Printf("--------------------------------------------------------------------------------\n")
	fmt.Printf("| %6s | %10s | %18s | %20s |\n", "HnswEf", "Avg. Recall", "Avg. Search Time", "Speedup (vs. Brute)")
	fmt.Printf("--------------------------------------------------------------------------------\n")

	for _, ef := range efValues {
		res := aggregatedResults[ef]
		if res.Count == 0 {
			continue
		}

		avgRecall := res.TotalRecall / float64(res.Count)
		avgSearchTime := res.TotalSearchTime / time.Duration(res.Count)

		// Calculate speedup: Brute Time / Qdrant Time
		speedup := float64(avgBruteTime) / float64(avgSearchTime)

		fmt.Printf("| %6d | %10.4f | %18v | %18.2fx |\n",
			ef, avgRecall, avgSearchTime, speedup)
	}
	fmt.Printf("--------------------------------------------------------------------------------\n")

	// Clean up
	_, _ = collectionsClient.Delete(ctx, &qdrant.DeleteCollection{CollectionName: CollectionName})
}
