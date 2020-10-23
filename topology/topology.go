package topology

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	"io"
)

type Node struct {
	freeCpus int
}

type DataCenter struct {
	nodes []*Node
}

func (dc DataCenter) Equal(other DataCenter) bool {
	if len(dc.nodes) != len(other.nodes) {
		return false
	}
	// TODO: this assumes that the other of nodes was not changed
	// it will possible require a fix
	for i := range dc.nodes {
		if dc.nodes[i].freeCpus != other.nodes[i].freeCpus {
			return false
		}
	}
	return true
}

type Topology struct {
	DataCenters []*DataCenter
	Speeds      [][]uint64
}

func New(capacity [][2]int, speeds [][]uint64) (*Topology, error) {
	var topo Topology
	topo.DataCenters = make([]*DataCenter, len(capacity))
	topo.Speeds = make([][]uint64, len(capacity))
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
		topo.Speeds[i] = make([]uint64, len(capacity))
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
		return nil, fmt.Errorf("failure to read topology: size error: %v", err)
	} else if n != 1 {
		return nil, fmt.Errorf("failure to read topology: size error: missing size")
	}

	capacity := make([][2]int, size)
	for i := 0; i < size; i++ {
		n, err := fmt.Fscanf(topoInfo, "%d%d", &capacity[i][0], &capacity[i][1])
		if err != nil {
			return nil, fmt.Errorf("failure to read topology: data center %v: %v", i, err)
		} else if n != 2 {
			return nil, fmt.Errorf("failure to read topology: data center %v: missing elements in capacity line", i)
		}
	}
	speeds := make([][]uint64, size)
	for i := 0; i < size; i++ {
		speeds[i] = make([]uint64, size)
		/*_, err := fmt.Fscanf(topoInfo, "\n")
		if err != nil {
			return nil, fmt.Errorf("failure to read topology: speeds %v: %v", i, err)
		}*/
		for k := 0; k < size; k++ {
			n, err := fmt.Fscanf(topoInfo, "%v", &speeds[i][k])
			if n != 1 {
				return nil, fmt.Errorf("failure to read topology: speeds %v: missing speeds", i)
			} else if err != nil {
				return nil, fmt.Errorf("failure to read topology: speeds %v: %v", i, err)
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

func (topo Topology) Equal(other Topology) bool {
	if len(topo.DataCenters) != len(other.DataCenters) {
		return false
	}
	for i := range topo.DataCenters {
		if !topo.DataCenters[i].Equal(*other.DataCenters[i]) {
			return false
		}
	}
	return cmp.Equal(topo.Speeds, other.Speeds)
}
