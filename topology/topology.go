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
	DataCenters []*DataCenter
	Speeds      [][]int
}

func New(capacity [][2]int, speeds [][]int) (*Topology, error) {
	var topo Topology
	topo.DataCenters = make([]*DataCenter, len(capacity))
	topo.Speeds = make([][]int, len(capacity))
	if len(speeds) != len(capacity) {
		return nil, fmt.Errorf("len(capacity)=%d != len(speeds)=%d", len(capacity), len(speeds))
	}
	for i, dc := range capacity {
		nNodes := dc[0]
		nCpus := dc[1]
		n := make([]*Node, nNodes)
		topo.DataCenters[i] = &DataCenter{nodes: n}
		for k := range topo.DataCenters[i].nodes {
			topo.DataCenters[i].nodes[k] = &Node{freeCpus: nCpus}
		}
		if len(speeds[i]) != len(capacity) {
			return nil, fmt.Errorf("len(capacity)=%d != len(speeds[%d])=%d", len(capacity), i, len(speeds))
		}
		topo.Speeds[i] = make([]int, len(capacity))
		for k := range speeds[i] {
			topo.Speeds[i][k] = speeds[i][k]
		}
	}
	return &topo, nil
}

func Load(topoInfo io.Reader) (*Topology, error) {
	var size int

	n, err := fmt.Fscanf(topoInfo, "%d", &size)
	if err != nil {
		return nil, fmt.Errorf("failure to read topology: %v", err)
	} else if n != 1 {
		return nil, fmt.Errorf("failure to read topology: missing size")
	}

	capacity := make([][2]int, size)
	for i := 0; i < size; i++ {
		n, err := fmt.Fscanf(topoInfo, "\n%d %d", &capacity[i][0], &capacity[i][1])
		if err != nil {
			return nil, fmt.Errorf("failure to read topology: %v", err)
		} else if n != 2 {
			return nil, fmt.Errorf("failure to read topology: missing elements in capacity line %d", i)
		}
	}
	speeds := make([][]int, size)
	for i := 0; i < size; i++ {
		speeds[i] = make([]int, size)
		_, err := fmt.Fscanf(topoInfo, "\n")
		if err != nil {
			return nil, fmt.Errorf("failure to read topology: %v", err)
		}
		for k := 0; k < size; k++ {
			n, err := fmt.Fscanf(topoInfo, "%d", &speeds[i][k])
			if n != 1 {
				return nil, fmt.Errorf("failure to read topology: missing speeds")
			} else if err != nil {
				return nil, fmt.Errorf("failure to read topology: %v", err)
			}
		}
	}
	// TODO: inspect here for proper validation of speeds

	return New(capacity, speeds)
}

func (n *Node) Host(cpus int) bool {
	if cpus <= n.freeCpus {
		n.freeCpus -= cpus
		return true
	}
	return false
}

func (n *Node) Free(cpus int) {
	n.freeCpus += cpus
}

func (dc *DataCenter) Host(cpus int) (*Node, bool) {
	for _, n := range dc.nodes {
		if n.Host(cpus) {
			return n, true
		}
	}
	return nil, false
}
