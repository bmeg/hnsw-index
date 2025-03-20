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
	m         uint8   // Max layers
	mMax      uint8   // Max neighbors per layer
	mMax0     uint8   // Max neighbors on layer 0
	efCount   int     // Number of friends in KNN construction
	dim       int     // Dimensions of stored vectors
	levelMult float64 // Multipler to calculate random layer
	Db        *DB

	epUpdateFreq uint32 // Update entry pointer after epUpdateFreq inserts
	epLayer      uint8  // Tracks what layer entry pointer is at

	// Entry pointer caching
	epID    uint64
	epVec   []float32
	epValid bool
}

type batchInsert struct {
	key, value []byte
}

func (graph *Graph) Insert(name []byte, vec []float32, batch *pebble.Batch) error {

	layer := uint8(math.Floor(-math.Log(rand.Float64()) * graph.levelMult))
	if layer > graph.m {
		layer = graph.m
	}

	eLayer, ep, eVec, err := graph.getOrUpdateEntryPoint()
	if err != nil {
		return err
	}

	if ep == 0 {
		nid, err := graph.Db.insertGraphVector(graph.graphid, name, vec, batch)
		if err != nil {
			return err
		}
		if nid != 1 {
			return fmt.Errorf("entrypoint init error")
		}
		graph.epLayer = graph.m
		graph.epID = nid
		graph.epVec = append([]float32(nil), vec...)
		graph.epValid = true
		if err := batch.Commit(pebble.Sync); err != nil { // Sync commit for first node
			return err
		}
		batch.Reset()
		return nil
	}

	id, err := graph.Db.insertGraphVector(graph.graphid, name, vec, batch)
	if err != nil {
		return err
	}
	if id == 0 {
		return fmt.Errorf("invalid node id (0) generated")
	}

	eDist := Euclidean(vec, eVec)
	if eLayer > layer+1 {
		for l := eLayer; l > layer+1; l-- {
			lf, err := graph.getLayerFriends(l, ep, graph.efCount)
			if err != nil {
				return err
			}
			fds, err := graph.getDistances(vec, lf)
			if err != nil {
				return err
			}
			for i, dist := range fds {
				if dist < eDist {
					ep = lf[i]
					eDist = dist
				}
			}
		}
	}

	inserts := make([]*batchInsert, 0, int(graph.mMax)*(int(layer)+1)*2)
	for l := int(layer); l >= 0; l-- {
		res, resDist, err := graph.layerSearch(vec, uint8(l), ep, graph.efCount)
		if err != nil {
			return err
		}
		maxNeighbors := graph.mMax
		if l == 0 {
			maxNeighbors = graph.mMax0
		}
		for i, r := range res {
			if i >= int(maxNeighbors) {
				break
			}
			kS, vS := graph.genInsertLink(graph.graphid, uint8(l), id, r, resDist[i])
			kD, vD := graph.genInsertLink(graph.graphid, uint8(l), r, id, resDist[i])
			inserts = append(inserts, &batchInsert{key: kS, value: vS}, &batchInsert{key: kD, value: vD})
		}
	}

	for _, i := range inserts {
		batch.Set(i.key, i.value, nil)
	}

	if id%uint64(graph.epUpdateFreq) == 0 {
		graph.epLayer, graph.epID, graph.epVec, _ = graph.updateEntryPoint()
		graph.epValid = true
	}
	return nil
}

func (graph *Graph) Search(vec []float32, K int, ef int) ([][]byte, error) {

	eDist := Euclidean(vec, graph.epVec)
	if graph.epLayer > 1 && eDist > 1.0 { // on higher layers if distance is 'close enough' use it
		for l := int(graph.epLayer); l > 0; l-- { // stop before reaching layer 0
			eFriends, err := graph.getLayerFriends(uint8(l), graph.epID, ef)
			if err != nil {
				return nil, err
			}
			fDists, err := graph.getDistances(vec, eFriends)
			if err != nil {
				return nil, err
			}
			for i := range eFriends {
				if fDists[i] < eDist {
					graph.epID = eFriends[i]
					eDist = fDists[i]
				}
			}
		}
	}

	ids, _, err := graph.layerSearch(vec, 0, graph.epID, ef)
	if err != nil {
		return nil, err
	}

	out := make([][]byte, 0, K)
	for i := 0; i < K && i < len(ids); i++ {
		n, err := graph.Db.getVectorName(graph.graphid, ids[i])
		if err == nil {
			out = append(out, n)
		}
	}
	return out, nil
}

func (graph *Graph) layerSearch(vec []float32, layer uint8, entryPoint uint64, K int) ([]uint64, []float32, error) {

	if entryPoint == 0 {
		return []uint64{}, []float32{}, fmt.Errorf("invalid entryPoint id")
	}

	visited := make(map[uint64]bool, K*10)
	candidates := distqueue.NewMin[float32, uint64]()
	w := distqueue.NewMinCapped[float32, uint64](K * 2)

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
		if cdist > fdist*1.5 && w.Filled() {
			break
		}
		neighbors, err := graph.getLayerFriends(layer, c, graph.efCount*2)
		if err != nil {
			return nil, nil, err
		}
		ndists, err := graph.getDistances(vec, neighbors)
		if err != nil {
			return nil, nil, err
		}
		for n := range neighbors {
			if !visited[neighbors[n]] {
				if ndists[n] < fdist || !w.Filled() {
					w.Insert(ndists[n], neighbors[n])
					candidates.Insert(ndists[n], neighbors[n])
					visited[neighbors[n]] = true
				}
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
	iter, err := graph.Db.Db.NewIter(&pebble.IterOptions{})
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
	out, closer, err := graph.Db.Db.Get(key)
	defer closer.Close()
	if err != nil {
		return nil, err
	}
	return VectorValueParse(out), nil
}

func (graph *Graph) getLayerFriends(l uint8, a uint64, count int) ([]uint64, error) {
	prefix := LayerKeyPrefixEncode(graph.graphid, l, a)

	iter, err := graph.Db.Db.NewIter(&pebble.IterOptions{LowerBound: prefix})
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
	out, closer, err := graph.Db.Db.Get(key)
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
		iter, err := graph.Db.Db.NewIter(&pebble.IterOptions{LowerBound: prefix})
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

func (graph *Graph) getOrUpdateEntryPoint() (uint8, uint64, []float32, error) {
	if graph.epValid {
		return graph.epLayer, graph.epID, graph.epVec, nil
	}
	eLayer, ep, eVec, err := graph.getEntryPoint()
	if err != nil {
		return 0, 0, nil, err
	}
	if ep != 0 {
		eLayer, ep, eVec, _ = graph.updateEntryPoint()
	}
	graph.epLayer = eLayer
	graph.epID = ep
	graph.epVec = eVec
	graph.epValid = true
	return eLayer, ep, eVec, nil
}

func (graph *Graph) updateEntryPoint() (uint8, uint64, []float32, error) {
	bestLayer := uint8(0)
	bestSrc := uint64(0)
	bestVec := []float32(nil)
	maxDegree := 0

	for l := graph.m; l >= 0; l-- {
		prefix := LayerPrefixEncode(graph.graphid, l)
		iter, err := graph.Db.Db.NewIter(&pebble.IterOptions{LowerBound: prefix})
		if err != nil {
			return 0, 0, nil, err
		}
		nodes := make(map[uint64]bool)
		edgeCount := 0
		for iter.First(); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
			_, _, src, _ := LayerKeyParse(iter.Key())
			dest := LayerValueParse(iter.Value()) // Get dest from value
			nodes[src] = true
			nodes[dest] = true
			edgeCount++
			friends, err := graph.getLayerFriends(l, src, int(graph.mMax*2))
			if err != nil {
				iter.Close()
				return 0, 0, nil, err
			}
			if len(friends) > maxDegree {
				maxDegree = len(friends)
				bestLayer = l
				bestSrc = src
				bestVec, _ = graph.GetVec(src)
			}
			if maxDegree >= int(graph.mMax) { // Early exit if decent degree found
				iter.Close()
				fmt.Printf("Updated entry point: layer=%d, id=%d, degree=%d\n", bestLayer, bestSrc, maxDegree)
				return bestLayer, bestSrc, bestVec, nil
			}
		}
		iter.Close()
		if maxDegree > 0 {
			break
		}
	}

	if maxDegree > 0 {
		fmt.Printf("Updated entry point: layer=%d, id=%d, degree=%d\n", bestLayer, bestSrc, maxDegree)
		return bestLayer, bestSrc, bestVec, nil
	}

	prefix := LayerPrefixEncode(graph.graphid, 0)
	iter, err := graph.Db.Db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	if err != nil {
		return 0, 0, nil, err
	}
	defer iter.Close()
	nodes := []uint64{}
	for iter.First(); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		_, _, src, _ := LayerKeyParse(iter.Key())
		if !contains(nodes, src) {
			nodes = append(nodes, src)
		}
	}
	if len(nodes) > 0 {
		randSrc := nodes[rand.IntN(len(nodes))]
		randVec, _ := graph.GetVec(randSrc)
		fmt.Printf("Fallback entry point: layer=0, id=%d (random)\n", randSrc)
		return 0, randSrc, randVec, nil
	}

	return graph.getEntryPoint()
}

func contains(slice []uint64, val uint64) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
