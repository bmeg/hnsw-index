package main_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// --- CONFIGURATION CONSTANTS ---
const (
	PGConnString   = "postgresql://vector_user:root@localhost:5432/vector_db"
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

// ptr creates a pointer to any value.
func ptr[T any](v T) *T {
	return &v
}

// GenerateBioVectors generates bio-inspired vectors clustered around random centers.
func GenerateBioVectors(numVectors, dim, numClusters int) map[string][]float32 {
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

// Euclidean computes the Euclidean distance between two vectors.
func Euclidean(v1, v2 []float32) float32 {
	var sum float64
	for i := range v1 {
		diff := float64(v1[i] - v2[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

// Neighbor represents a neighbor with ID and distance.
type Neighbor struct {
	ID   string
	Dist float32
}

// getBruteForceNeighbors computes the top K nearest neighbors using brute force.
func getBruteForceNeighbors(queryVec []float32, vmap map[string][]float32, K int) []string {
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

// computeRecall calculates the recall of search results against ground truth.
func computeRecall(trueNeighbors []string, searchResults []string, K int) float64 {
	trueSet := make(map[string]struct{})
	for _, id := range trueNeighbors {
		trueSet[id] = struct{}{}
	}
	hits := 0
	for _, id := range searchResults {
		if _, found := trueSet[id]; found {
			hits++
		}
	}
	return float64(hits) / float64(K)
}

// formatVector converts []float32 to a PostgreSQL vector string.
func formatVector(v []float32) string {
	vals := make([]string, len(v))
	for i, val := range v {
		vals[i] = fmt.Sprintf("%f", val)
	}
	return fmt.Sprintf("[%s]", join(vals, ","))
}

// join is a simple string join function (since strings.Join is not used for clarity).
func join(elems []string, sep string) string {
	if len(elems) == 0 {
		return ""
	}
	result := elems[0]
	for _, e := range elems[1:] {
		result += sep + e
	}
	return result
}

// ---------------------- MAIN TEST FUNCTION ----------------------
func TestPGVectorBioBenchmark(t *testing.T) {
	NumClusters := 10

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Connect to PostgreSQL
	conn, err := pgx.Connect(ctx, PGConnString)
	if err != nil {
		t.Fatalf("Could not connect to PostgreSQL at %s: %v", PGConnString, err)
	}
	defer conn.Close(ctx)

	// Clean up table if it exists
	_, err = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", CollectionName))
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// 1. Create Table with Vector Column and HNSW Index
	fmt.Println("Creating table...")
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT PRIMARY KEY,
			vector vector(%d)
		);
		CREATE INDEX ON %s USING hnsw (vector vector_l2_ops);
	`, CollectionName, Dim, CollectionName))
	if err != nil {
		t.Fatalf("Failed to create table or index: %v", err)
	}
	fmt.Printf("Table '%s' created (Dim=%d, Distance=Euclidean)\n", CollectionName, Dim)

	// 2. Generate Vectors and Insert Data
	fmt.Println("Generating and inserting vectors...")
	vmap := GenerateBioVectors(NumVectors, Dim, NumClusters)

	start := time.Now()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	for idStr, v := range vmap {
		id, _ := strconv.ParseInt(idStr, 10, 64)
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, vector) VALUES ($1, $2)
		`, CollectionName), id, formatVector(v))
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("Failed to insert vector %s: %v", idStr, err)
		}
	}
	err = tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	insertTime := time.Since(start)
	fmt.Printf("Inserted %d vectors in %v\n", NumVectors, insertTime)

	// 3. Run Benchmark Queries and Aggregate Results
	fmt.Println("\nStarting HNSW Recall Benchmark...")
	aggregatedResults := make(map[int]*BenchmarkResult)
	for _, ef := range efValues {
		aggregatedResults[ef] = &BenchmarkResult{HnswEf: ef}
	}

	var totalBruteTime time.Duration

	for q := 0; q < NumQueries; q++ {
		qID := fmt.Sprintf("%d", rand.Intn(NumVectors))
		qVec := vmap[qID]

		// Brute-Force Time Calculation (Ground Truth)
		start = time.Now()
		trueNeighbors := getBruteForceNeighbors(qVec, vmap, K)
		bruteTime := time.Since(start)
		totalBruteTime += bruteTime

		for _, ef := range efValues {
			// Set ef_search for this query
			_, err := conn.Exec(ctx, fmt.Sprintf("SET hnsw.ef_search = %d", ef))
			if err != nil {
				t.Fatalf("Failed to set ef_search to %d: %v", ef, err)
			}

			// Execute PGVector Search
			start = time.Now()
			rows, err := conn.Query(ctx, fmt.Sprintf(`
				SELECT id FROM %s
				ORDER BY vector <-> $1
				LIMIT $2
			`, CollectionName), formatVector(qVec), K)
			if err != nil {
				t.Fatalf("Error during Search (Query %d, EF %d): %v", q+1, ef, err)
			}

			searchResults := make([]string, 0, K)
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					t.Fatalf("Error scanning result: %v", err)
				}
				searchResults = append(searchResults, strconv.FormatInt(id, 10))
			}
			rows.Close()
			searchTime := time.Since(start)

			recall := computeRecall(trueNeighbors, searchResults, K)

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

	fmt.Printf("\n--- PGVECTOR HNSW PERFORMANCE VS. BRUTE-FORCE ---\n")
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
		speedup := float64(avgBruteTime) / float64(avgSearchTime)

		fmt.Printf("| %6d | %10.4f | %18v | %18.2fx |\n",
			ef, avgRecall, avgSearchTime, speedup)
	}
	fmt.Printf("--------------------------------------------------------------------------------\n")

	// Clean up
	_, err = conn.Exec(ctx, fmt.Sprintf("DROP TABLE %s", CollectionName))
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}
