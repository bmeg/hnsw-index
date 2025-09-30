# Vector Database VS PGVECTOR

Same Load, same query across both PGVector and Qdrant:

```
NumVectors = 40000
Dim = 512
NumQueries = 5
K = 100 // Top K neighbors to fetch
```

## PG Vector

=== RUN TestPGVectorBioBenchmark
Creating table...
Table 'bio_benchmark_collection' created (Dim=512, Distance=Euclidean)
Generating and inserting vectors...

Inserted 40000 vectors in 3m43.713291792s

Starting HNSW Recall Benchmark...
--- BENCHMARK COMPLETE ---
Total Queries Run: 5
Brute-Force Baseline (Avg. Search Time): 2.585995783s

--- PGVECTOR HNSW PERFORMANCE VS. BRUTE-FORCE ---
| HnswEf | Avg. Recall | Avg. Search Time | Speedup (vs. Brute) |
| 10 | 0.1000 | 3.371166ms | 767.09x |
| 20 | 0.2000 | 1.707491ms | 1514.50x |
| 50 | 0.5000 | 2.590408ms | 998.30x |
| 100 | 0.9100 | 3.396066ms | 761.47x |

---

--- PASS: TestPGVectorBioBenchmark (237.09s)
PASS

## Qdrant

test % go test -v qdrant_test.go

=== RUN TestQdrantBioBenchmark
Creating collection...
Collection 'bio_benchmark_collection' created (Dim=512, Distance=Euclidean)
Generating and inserting vectors...
Inserted 40000 vectors in 1.266104625s (Index build complete)

Starting HNSW Recall Benchmark...
--- BENCHMARK COMPLETE ---
Total Queries Run: 5
Brute-Force Baseline (Avg. Search Time): 2.651691642s

--- QDRANT HNSW PERFORMANCE VS. BRUTE-FORCE ---
| HnswEf | Avg. Recall | Avg. Search Time | Speedup (vs. Brute) |
| 10 | 0.9100 | 8.096316ms | 327.52x |
| 20 | 0.9100 | 4.297866ms | 616.98x |
| 50 | 0.9100 | 3.961366ms | 669.39x |
| 100 | 0.9100 | 4.389558ms | 604.09x |

---

--- PASS: TestQdrantBioBenchmark (15.10s)

Even at 40k vectors, PGVector is starting to struggle (3 min insertion time) and vector database insertion time increases as the number of vectors in the collection increases...
