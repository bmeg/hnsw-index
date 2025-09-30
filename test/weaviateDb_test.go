package test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// GenerateBioVectors (unchanged)
func genBioVectors(numVectors, dim, numClusters int) map[uint64][]float32 {
	vmap := make(map[uint64][]float32, numVectors)
	rand.Seed(time.Now().UnixNano())

	centers := make([][]float32, numClusters)
	for c := 0; c < numClusters; c++ {
		center := make([]float32, dim)
		for j := 0; j < dim; j++ {
			center[j] = rand.Float32() * 100
		}
		centers[c] = center
	}

	for i := 0; i < numVectors; i++ {
		c := make([]float32, dim)
		cluster := rand.Intn(numClusters)
		for j := 0; j < dim; j++ {
			noise := float32(rand.NormFloat64()) * 0.5
			c[j] = centers[cluster][j] + noise
		}
		vmap[uint64(i)] = c
	}
	return vmap
}

// NewStore creates a persistent LSMKV store as Weaviate does in production
func NewStore(rootDir string, logger logrus.FieldLogger) (*lsmkv.Store, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %v", rootDir, err)
	}
	compactCallbacks := cyclemanager.NewCallbackGroup("compact", logger, 1)
	flushCallbacks := cyclemanager.NewCallbackGroup("flush", logger, 1)
	tombstoneCallbacks := cyclemanager.NewCallbackGroup("tombstone", logger, 1)

	store, err := lsmkv.New(
		rootDir,            // Root directory for the store
		rootDir,            // WAL directory (using same as root for simplicity)
		logger,             // Logger instance
		nil,                // No recovery callback for this example
		compactCallbacks,   // Compaction callbacks
		flushCallbacks,     // Flush callbacks
		tombstoneCallbacks, // Tombstone cleanup callbacks
	)

	if err != nil {
		return nil, err
	}
	return store, err
}

// BenchmarkHNSW benchmarks the HNSW index with varying ef configurations
func BenchmarkHNSW(b *testing.B) {
	// Parameters
	numVectors := 40000
	dim := 512
	numClusters := 10
	K := 10
	efConfigs := []struct {
		efMin int
		efMax int
	}{
		{20, 100},
	}

	// Generate vectors once
	vmap := genBioVectors(numVectors, dim, numClusters)

	b.ResetTimer()

	for _, ef := range efConfigs {
		b.Run(fmt.Sprintf("efMin=%d_efMax=%d", ef.efMin, ef.efMax), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ctx := context.Background()

				// Initialize a real logger
				logger := logrus.New()
				logger.SetLevel(logrus.InfoLevel) // Adjust to Debug for more verbosity

				// Create a unique root path
				rootPath := filepath.Join("testdata", fmt.Sprintf("bio_index_%d_%d_%d", ef.efMin, ef.efMax, i))
				store, err := NewStore(rootPath, logger)
				if err != nil {
					b.Fatalf("failed to create store: %v", err)
				}

				makeCL := func() (hnsw.CommitLogger, error) {
					return hnsw.NewCommitLogger(rootPath, "bio_graph", logger, cyclemanager.NewCallbackGroup("commitLoggerThunk", logger, 1))
				}

				index, err := hnsw.New(hnsw.Config{
					RootPath:              rootPath,
					ID:                    "bio_graph",
					MakeCommitLoggerThunk: makeCL,
					DistanceProvider:      distancer.NewL2SquaredProvider(),
					VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
						if vec, ok := vmap[id]; ok {
							return vec, nil
						}
						return nil, fmt.Errorf("vector for ID %d not found", id)
					},
				}, ent.UserConfig{
					CleanupIntervalSeconds: 10,
					VectorCacheMaxObjects:  1000000,
					Distance:               "l2-squared",
					DynamicEFMin:           ef.efMin,
					DynamicEFMax:           ef.efMax,
					DynamicEFFactor:        8,
					EFConstruction:         200,
					MaxConnections:         10,
				}, cyclemanager.NewCallbackGroup("cml", logger, 1), store)
				if err != nil {
					b.Fatalf("failed to create HNSW index with efMin=%d, efMax=%d: %v", ef.efMin, ef.efMax, err)
				}

				// Insert vectors
				start := time.Now()
				for id, vec := range vmap {
					if err := index.Add(ctx, id, vec); err != nil {
						b.Fatalf("failed to add vector %d: %v", id, err)
					}
				}
				insertTime := time.Since(start)
				b.Logf("Inserted %d vectors with efMin=%d, efMax=%d in %v (%.2f vectors/sec)", len(vmap), ef.efMin, ef.efMax, insertTime, float64(len(vmap))/insertTime.Seconds())

				// Test queries
				numQueries := 10
				recallSum := 0.0

				for q := 0; q < numQueries; q++ {
					qID := rand.Int63n(int64(numVectors))
					qVec := vmap[uint64(qID)]

					// Brute-force ground truth with timing
					type neighbor struct {
						id   uint64
						dist float32
					}
					neighbors := make([]neighbor, 0, numVectors)
					bruteStart := time.Now()
					for id, vec := range vmap {
						dist, _ := distancer.NewL2SquaredProvider().SingleDist(vec, qVec)
						neighbors = append(neighbors, neighbor{id: id, dist: dist})
					}
					sort.Slice(neighbors, func(i, j int) bool {
						return neighbors[i].dist < neighbors[j].dist
					})
					trueNeighbors := make([]uint64, K)
					for i := 0; i < K && i < len(neighbors); i++ {
						trueNeighbors[i] = neighbors[i].id
					}
					bruteTime := time.Since(bruteStart)

					// HNSW Search
					hnswStart := time.Now()
					results, _, err := index.SearchByVector(ctx, qVec, K, nil)
					if err != nil {
						b.Fatalf("search failed: %v", err)
					}
					hnswTime := time.Since(hnswStart)

					// Compute recall
					hits := 0
					for _, result := range results {
						for _, trueID := range trueNeighbors {
							if result == trueID {
								hits++
								break
							}
						}
					}
					recall := float64(hits) / float64(K)
					recallSum += recall

					// Log both times and speedup
					speedup := float64(bruteTime) / float64(hnswTime)
					b.Logf("Query %d, efMin=%d, efMax=%d: HNSW found %d/%d true neighbors, Recall=%.3f, HNSW time=%v, Brute time=%v, Speedup=%.2fx",
						q, ef.efMin, ef.efMax, hits, K, recall, hnswTime, bruteTime, speedup)
				}

				avgRecall := recallSum / float64(numQueries)
				b.Logf("Average Recall across %d queries with efMin=%d, efMax=%d: %.3f", numQueries, ef.efMin, ef.efMax, avgRecall)

			}
		})
	}
}
