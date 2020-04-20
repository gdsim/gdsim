package topology

import (
	"fmt"
	"io"
)

type Node struct {
	freeCpus int
}

type DataCenter struct {
	nodes []*Node
}

type Topology struct {
	dataCenters []*DataCenter
	speeds      [][]int
}

func New(capacity [][2]int, speeds [][]int) (*Topology, error) {
	var topo Topology
	topo.dataCenters = make([]*DataCenter, len(capacity))
	topo.speeds = make([][]int, len(capacity))
	if len(speeds) != len(capacity) {
		return nil, fmt.Errorf("len(capacity)=%d != len(speeds)=%d", len(capacity), len(speeds))
	}
	for i, dc := range capacity {
		nNodes := dc[0]
		nCpus := dc[1]
		n := make([]*Node, nNodes)
		topo.dataCenters[i] = &DataCenter{nodes: n}
		for k := range topo.dataCenters[i].nodes {
			topo.dataCenters[i].nodes[k] = &Node{freeCpus: nCpus}
		}
		if len(speeds[i]) != len(capacity) {
			return nil, fmt.Errorf("len(capacity)=%d != len(speeds[%d])=%d", len(capacity), i, len(speeds))
		}
		topo.speeds[i] = make([]int, len(capacity))
	}
	return &topo, nil
}

func Load(topoInfo io.Reader) (*Topology, error) {
	var size int

	n, err := fmt.Fscanf(topoInfo, "%d", &size)
	if err != nil {
		return nil, fmt.Errorf("failure to read topology: %x", err)
	} else if n != 1 {
		return nil, fmt.Errorf("failure to read topology: missing size")
	}

	capacity := make([][2]int, size)
	for i := 0; i < size; i++ {
		n, err := fmt.Fscanf(topoInfo, "%d %d", &capacity[i][0], &capacity[i][1])
		if err != nil {
			return nil, fmt.Errorf("failure to read topology: %x", err)
		} else if n != 2 {
			return nil, fmt.Errorf("failure to read topology: missing elements in capacity line %d", i)
		}
	}
	speeds := make([][]int, size)
	for i := 0; i < size; i++ {
		speeds[i] = make([]int, size)
		for k := 0; k < size; k++ {
			fmt.Fscanf(topoInfo, "%d", &speeds[i][k])
		}
	}

	return New(capacity, speeds)
}
