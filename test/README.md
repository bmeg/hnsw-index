To run weavite tests
`go test -bench=. -benchmem -v -benchtime=1x ./weaviateDb_test.go`

## PG VECTOR SETUP:

```
docker run -d --name pgvector \
  -e POSTGRES_PASSWORD=<your_password> \
  -e POSTGRES_DB=vector_db \
  -e POSTGRES_USER=vector_user \
  -p 5432:5432 \
  --cpus=4 --memory=8g \
  ankane/pgvector


docker exec -it pgvector psql -U vector_user -d vector_db

CREATE EXTENSION vector;

SET work_mem = '64MB';
SET maintenance_work_mem = '512MB';
SET max_parallel_workers_per_gather = 4;
SET synchronous_commit = off;
SET wal_compression = on;

exit

go mod tidy
go test -v pg_vector_test.go
```
