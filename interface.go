package hnswindex

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/bmeg/hnsw-index/distqueue"
	"github.com/cockroachdb/pebble"
)

type Node struct {
	Id     []byte
	Vector []float32
}

type Graph struct {
	graphid   uint32
	name      string
	m         uint8   //number of layers
	efCount   int     //number of friends in KNN construction
	dim       int     //dimensions of stored vectors
	levelMult float64 //multipler to calculate random layer
	db        *DB
}

type batchInsert struct {
	key, value []byte
}

func (graph *Graph) Insert(name []byte, vec []float32) error {

	layer := uint8(math.Floor(-math.Log(rand.Float64() * graph.levelMult)))

	//fmt.Printf("Insert Layer: %d\n", layer)

	eLayer, ep, eVec, err := graph.getEntryPoint()
	if err != nil {
		return err
	}

	if ep == 0 {
		//no entrypoint, so this node becomes it
		nid, err := graph.db.insertGraphVector(graph.graphid, name, vec)
		if err != nil {
			return err
		}
		if nid != 1 {
			return fmt.Errorf("entrypoint init error")
		}
		//fmt.Printf("entrypoint id: %d\n", nid)
		return nil
	}

	id, err := graph.db.insertGraphVector(graph.graphid, name, vec)
	if err != nil {
		return err
	}
	if id == 0 {
		return fmt.Errorf("invalid node id (0) generated")
	}

	eDist := Euclidean(vec, eVec)

	//move down the layers to the target layer, attempting to get close along the way
	for l := eLayer; l > layer+1; l-- {
		changed := true
		for changed {
			changed = false
			lf, err := graph.getLayerFriends(eLayer, ep, graph.efCount)
			if err != nil {
				return err
			}
			fds, err := graph.getDistances(vec, lf)
			if err != nil {
				return err
			}
			for i := range lf {
				if fds[i] < eDist {
					ep = lf[i]
					eDist = fds[i]
					changed = true
				}
			}
		}
	}

	inserts := make([]*batchInsert, 0, 100)
	for l := int(layer); l >= 0; l-- {
		res, resDist, err := graph.layerSearch(vec, uint8(l), ep, graph.efCount)
		if err != nil {
			return err
		}
		//fmt.Printf("Layer Search %#v %#v\n", res, resDist)
		//record links for current layer
		for i := range res {
			//fmt.Printf("Inserting link: %d %d %d %f\n", l, id, res[i], resDist[i])
			kS, vS := graph.genInsertLink(graph.graphid, uint8(l), id, res[i], resDist[i])
			kD, vD := graph.genInsertLink(graph.graphid, uint8(l), res[i], id, resDist[i])
			inserts = append(inserts, &batchInsert{key: kS, value: vS}, &batchInsert{key: kD, value: vD})
		}
	}

	batch := graph.db.db.NewBatch()
	for _, i := range inserts {
		batch.Set(i.key, i.value, nil)
	}
	batch.Commit(nil)
	batch.Close()

	return nil
}

func (graph *Graph) Search(vec []float32, K int, ef int) ([][]byte, error) {
	eLevel, ePoint, eVec, err := graph.getEntryPoint()
	if err != nil {
		return nil, err
	}

	eDist := Euclidean(vec, eVec)
	for l := int(eLevel); l >= 0; l-- {
		changed := true
		for changed {
			changed = false
			eFriends, err := graph.getLayerFriends(uint8(l), ePoint, graph.efCount)
			if err != nil {
				return nil, err
			}
			fDists, err := graph.getDistances(vec, eFriends)
			if err != nil {
				return nil, err
			}

			for i := range eFriends {
				if fDists[i] < eDist {
					ePoint = eFriends[i]
					eDist = fDists[i]
					changed = true
				}
			}
		}
	}

	ids, _, err := graph.layerSearch(vec, 0, ePoint, ef)
	if err != nil {
		return nil, err
	}

	out := make([][]byte, 0, K)
	for i := 0; i < K && i < len(ids); i++ {
		n, err := graph.db.getVectorName(graph.graphid, ids[i])
		if err == nil {
			out = append(out, n)
		}
	}
	return out, nil
}

/*

func (graph *Graph) FindLayerEntryPoint(layer uint8) (uint64, error) {
	prefix := LayerPrefixEncode(graph.graphid, layer)
	iter, err := graph.db.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	iter.SeekGE(prefix)
	if iter.Valid() && bytes.HasPrefix(iter.Key(), prefix) {
		_, _, source, _ := LayerKeyParse(iter.Key())
		return source, nil
	}
	return 0, nil
}

*/

func (graph *Graph) layerSearch(vec []float32, layer uint8, entryPoint uint64, K int) ([]uint64, []float32, error) {

	if entryPoint == 0 {
		return []uint64{}, []float32{}, fmt.Errorf("invalid entryPoint id")
	}

	visited := map[uint64]bool{}
	candidates := distqueue.NewMin[float32, uint64]()
	w := distqueue.NewMinCapped[float32, uint64](K)

	eVec, err := graph.GetVec(entryPoint)
	if err != nil {
		return nil, nil, err
	}

	d := Euclidean(vec, eVec)
	w.Insert(d, entryPoint)
	candidates.Insert(d, entryPoint)
	visited[entryPoint] = true

	for len(candidates) > 0 {
		cdist, c := candidates.Pop()
		fdist := w.Max()
		if cdist > fdist {
			break
		}
		neighbors, err := graph.getLayerFriends(layer, c, graph.efCount)
		if err != nil {
			return nil, nil, err
		}
		ndists, err := graph.getDistances(vec, neighbors)
		if err != nil {
			return nil, nil, err
		}
		for n := range neighbors {
			if _, ok := visited[neighbors[n]]; !ok {
				if len(w) > 0 {
					fdist := w.Max()
					if (ndists[n] < fdist) || !w.Filled() {
						w.Insert(ndists[n], neighbors[n])
						candidates.Insert(ndists[n], neighbors[n])
					}
				}
				visited[neighbors[n]] = true
			}
		}
	}
	outI := make([]uint64, len(w))
	outD := make([]float32, len(w))
	for i := range w {
		outI[i] = w[i].Value
		outD[i] = w[i].Dist

	}
	return outI, outD, nil
}

func (graph *Graph) getDistances(v []float32, n []uint64) ([]float32, error) {
	out := make([]float32, len(n))
	iter, err := graph.db.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for i := range n {
		key := VectorKeyEncode(graph.graphid, n[i])
		if iter.SeekGE(key) {
			if bytes.Equal(iter.Key(), key) {
				out[i] = Euclidean(v, VectorValueParse(iter.Value()))
			}
		}
	}
	return out, nil
}

func (graph *Graph) GetVec(id uint64) ([]float32, error) {

	key := VectorKeyEncode(graph.graphid, id)
	out, closer, err := graph.db.db.Get(key)
	defer closer.Close()
	if err != nil {
		return nil, err
	}
	return VectorValueParse(out), nil
}

func (graph *Graph) getLayerFriends(l uint8, a uint64, count int) ([]uint64, error) {
	prefix := LayerKeyPrefixEncode(graph.graphid, l, a)

	iter, err := graph.db.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := make([]uint64, 0, 10)
	i := 0
	for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix) && i < count; iter.Next() {
		dest := LayerValueParse(iter.Value())
		out = append(out, dest)
		i++
	}
	return out, nil
}

func (graph *Graph) getEntryPoint() (uint8, uint64, []float32, error) {
	key := VectorKeyEncode(graph.graphid, 1)
	out, closer, err := graph.db.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, 0, nil, nil
		}
		return 0, 0, nil, err
	}
	defer closer.Close()
	v := VectorValueParse(out)
	return graph.m, 1, v, nil
}

type LayerEdge struct {
	Source, Dest uint64
	Dist         float32
}

func (graph *Graph) ListLayer(layer uint8) chan *LayerEdge {

	out := make(chan *LayerEdge, 10)

	go func() {
		defer close(out)
		prefix := LayerPrefixEncode(graph.graphid, layer)
		iter, err := graph.db.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
		if err != nil {
			return
		}
		defer iter.Close()
		for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
			_, _, src, dist := LayerKeyParse(iter.Key())
			dest := LayerValueParse(iter.Value())
			out <- &LayerEdge{
				Source: src, Dest: dest, Dist: dist,
			}
		}
	}()

	return out

}

func (graph *Graph) genInsertLink(graphId uint32, layer uint8, src uint64, dst uint64, dist float32) ([]byte, []byte) {
	key := LayerKeyEncode(graphId, layer, src, dist)
	value := LayerValueEncode(dst)
	return key, value
}
