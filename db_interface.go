package hnswindex

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble"
)

type DB struct {
	Db *pebble.DB
}

func New(path string) (*DB, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func (db *DB) Close() {
	db.Db.Close()
}

func (db *DB) NewGraph(name string, dim int, M uint8, mMax uint8, mMax0 uint8, efCount int, epUpdateFreq uint32) (*Graph, error) {
	h := Graph{
		name:         name,
		m:            M,
		mMax:         mMax,
		mMax0:        mMax0,
		Db:           db,
		dim:          dim,
		efCount:      efCount,
		levelMult:    1 / math.Log(float64(mMax)),
		epUpdateFreq: epUpdateFreq,
	}
	return &h, nil
}

func (db *DB) insertGraphVector(graphid uint32, name []byte, vec []float32, batch *pebble.Batch) (uint64, error) {
	nameKey := NameKeyEncode(graphid, name)
	nameId, err := db.newVectorID(graphid, batch)
	if err != nil {
		return 0, err
	}
	nameValue := NameValueEncode(nameId)
	batch.Set(nameKey, nameValue, nil)

	vecKey := VectorKeyEncode(graphid, nameId)
	vecValue := VectorValueEncode(vec)
	batch.Set(vecKey, vecValue, nil)
	batch.Set(NameRevKeyEncode(graphid, nameId), name, nil)

	return nameId, nil
}

func (db *DB) newVectorID(graphId uint32, batch *pebble.Batch) (uint64, error) {
	counterKey := []byte(fmt.Sprintf("graph:%d:nextid", graphId))
	val, closer, err := db.Db.Get(counterKey)
	var nextID uint64
	if err == pebble.ErrNotFound {
		nextID = 1
	} else if err != nil {
		return 0, err
	} else {
		defer closer.Close()
		nextID = binary.BigEndian.Uint64(val)
	}
	newID := nextID
	nextID++
	batch.Set(counterKey, encodeUint64(nextID), nil) // Defer commit to caller
	return newID, nil
}

func encodeUint64(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return b
}

func (db *DB) getVectorName(graphId uint32, id uint64) ([]byte, error) {

	key := NameRevKeyEncode(graphId, id)
	val, closer, err := db.Db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}
