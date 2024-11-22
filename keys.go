package hnswindex

import (
	"encoding/binary"
	"math"
)

//Key types

// graph
// desc : graph name to fixed int id
// key: byte graphPrefix, []byte name
// value : int32 id, int32 M, int32 vectorSize

var graphPrefix byte = 'g'

func GraphKeyEncode(name []byte) []byte {
	out := make([]byte, len(name)+1)
	out[0] = graphPrefix
	for i := 0; i < len(name); i++ {
		out[i+1] = name[i]
	}
	return out
}

func GraphKeyParse(key []byte) []byte {
	//duplicate the key, because pebble reuses memory
	out := make([]byte, len(key)-1)
	for i := 0; i < len(key)-1; i++ {
		out[i] = key[i+1]
	}
	return out
}

// name
// desc : entry name to fixed int id
// key: int32 graphId, []byte name
// value: int64 id

var namePrefix byte = 'e'

func NameKeyEncode(graphId uint32, name []byte) []byte {
	out := make([]byte, len(name)+5)
	out[0] = namePrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	for i := 0; i < len(name); i++ {
		out[i+5] = name[i]
	}
	return out
}

func NameKeyParse(key []byte) (uint32, []byte) {
	//duplicate the key, because pebble reuses memory
	out := make([]byte, len(key)-1)
	graphId := binary.LittleEndian.Uint32(key[1:])
	for i := 0; i < len(key)-5; i++ {
		out[i] = key[i+5]
	}
	return graphId, out
}

func NameValueEncode(vecId uint64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, vecId)
	return out
}

func NameGraphPrefix(graphId uint32) []byte {
	out := make([]byte, 5)
	out[0] = namePrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	return out
}

// nameRev
// desc : entry id back to original name
// key: int32 graphId, uint65 nameId
// value: []byte name
var nameRevPrefix byte = 'E'

func NameRevKeyEncode(graphId uint32, nameId uint64) []byte {
	out := make([]byte, 13)
	out[0] = nameRevPrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	binary.LittleEndian.PutUint64(out[5:], nameId)
	return out
}

func NameRevKeyParse(key []byte) (uint32, uint64) {
	graphId := binary.LittleEndian.Uint32(key[1:])
	nameId := binary.LittleEndian.Uint64(key[5:])
	return graphId, nameId
}

func NameRevGraphPrefix(graphId uint32) []byte {
	out := make([]byte, 5)
	out[0] = nameRevPrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	return out
}

// vector
// desc: vector value of entry
// key: int32 graphID, int64 entryID
// value: []float32 vector

var vectorPrefix byte = 'v'

func VectorKeyEncode(graphId uint32, entry uint64) []byte {
	// prefix (1 byte) + graphId (4 bytes) + source (8 bytes)
	out := make([]byte, 13)
	out[0] = vectorPrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	binary.LittleEndian.PutUint64(out[5:], entry)
	return out
}

func VectorValueEncode(vec []float32) []byte {
	out := make([]byte, len(vec)*4)
	for i := 0; i < len(vec); i++ {
		binary.BigEndian.PutUint32(out[i*4:], math.Float32bits(vec[i]))
	}
	return out
}

func VectorValueParse(val []byte) []float32 {
	out := make([]float32, len(val)/4)
	for i := 0; i < len(val)/4; i++ {
		out[i] = math.Float32frombits(binary.BigEndian.Uint32(val[i*4:]))
	}
	return out
}

// layer
// desc: layer values, connecting top M edges for each vertex for layer L
// key: int32 graphID, int64 source, float32 distance
// value: int64 destination

var layerPrefix byte = 'l'

func LayerKeyEncode(graphId uint32, layer uint8, source uint64, dist float32) []byte {
	// prefix (1 byte) + graphId (4 bytes) + layer (1 byte) + source (8 bytes) + dist (4 bytes)
	out := make([]byte, 18)
	out[0] = layerPrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	out[5] = layer
	binary.LittleEndian.PutUint64(out[6:], source)
	// bigEndian encode a 32bit float so it is sorted correctly
	binary.BigEndian.PutUint32(out[14:], math.Float32bits(dist))
	return out
}

func LayerKeyParse(key []byte) (uint32, uint8, uint64, float32) {
	return binary.LittleEndian.Uint32(key[1:]),
		uint8(key[5]),
		binary.LittleEndian.Uint64(key[6:]),
		math.Float32frombits(binary.BigEndian.Uint32((key[14:])))
}

func LayerKeyPrefixEncode(graphId uint32, layer uint8, source uint64) []byte {
	out := make([]byte, 14)
	out[0] = layerPrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	out[2] = layer
	binary.LittleEndian.PutUint64(out[6:], source)
	return out
}

func LayerPrefixEncode(graphId uint32, layer uint8) []byte {
	out := make([]byte, 6)
	out[0] = layerPrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	out[5] = layer
	return out
}

func LayerGraphPrefixEncode(graphId uint32) []byte {
	out := make([]byte, 5)
	out[0] = layerPrefix
	binary.LittleEndian.PutUint32(out[1:], graphId)
	return out
}

func LayerValueEncode(dest uint64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, dest)
	return out
}

func LayerValueParse(value []byte) uint64 {
	return binary.LittleEndian.Uint64(value)
}
