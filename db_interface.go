package hnswindex

import (
	"bytes"
	"math"

	"github.com/cockroachdb/pebble"
)

type DB struct {
	db *pebble.DB
}

func New(path string) (*DB, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func (db *DB) Close() {
	db.db.Close()
}

func (db *DB) NewGraph(name string, dim int, M uint8, efCount int) (*Graph, error) {
	h := Graph{name: name, m: M, db: db, dim: dim, efCount: efCount}
	// default values used in c++ implementation
	h.levelMult = 1 / math.Log(float64(M))
	return &h, nil
}

func (db *DB) insertGraphVector(graphid uint32, name []byte, vec []float32) (uint64, error) {
	//TODO: add mutex
	nameKey := NameKeyEncode(graphid, name)
	nameId, err := db.newVectorID(graphid)
	if err != nil {
		return 0, err
	}
	nameValue := NameValueEncode(nameId)

	db.db.Set(nameKey, nameValue, nil)

	vecKey := VectorKeyEncode(graphid, nameId)
	vecValue := VectorValueEncode(vec)
	db.db.Set(vecKey, vecValue, nil)
	db.db.Set(NameRevKeyEncode(graphid, nameId), name, nil)

	return nameId, nil
}

func (db *DB) newVectorID(graphId uint32) (uint64, error) {
	prefix := NameRevGraphPrefix(graphId)
	iter, err := db.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	//TODO: there is probably a more efficient way to do this, like starting above and going backward
	maxID := uint64(0)
	for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		_, maxID = NameRevKeyParse(iter.Key())
	}
	return maxID + 1, nil
}

func (db *DB) getVectorName(graphId uint32, id uint64) ([]byte, error) {

	key := NameRevKeyEncode(graphId, id)
	val, closer, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}
