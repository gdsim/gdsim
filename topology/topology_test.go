package topology

import (
	"testing"
)

func TestNew(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 2},
		[2]int{2, 1},
		[2]int{3, 5},
		[2]int{1, 3},
	}
	speed := [][]int{
		[]int{0, 1, 1, 1},
		[]int{1, 0, 1, 1},
		[]int{1, 1, 0, 1},
		[]int{1, 1, 1, 0},
	}

	topo, err := New(cap, speed)
	if err != nil {
		t.Errorf("expected err = nil, found %v", err)
	}
	if len(topo.DataCenters) != len(cap) {
		t.Errorf("expected len(topo.DataCenters) == %d, found %d", len(cap), len(topo.DataCenters))
	}

	for i, dc := range topo.DataCenters {
		if cap[i][0] != len(dc.nodes) {
			t.Errorf("expected len(DataCenter[%d]) = %d, found %d", i, cap[i][0], len(dc.nodes))
		}
		for k, n := range dc.nodes {
			if cap[i][1] != n.freeCpus {
				t.Errorf("expected node[%d].freeCpus = %d, found %d", k, cap[i][1], n.freeCpus)
			}
		}
	}

	badSpeed := [][]int{
		[]int{0, 1, 1, 1},
		[]int{1, 0, 1, 1},
		[]int{1, 1, 0, 1},
		[]int{1, 1, 1, 0},
		[]int{1, 1, 1, 0},
	}
	_, err = New(cap, badSpeed)
	if err == nil {
		t.Errorf("expected err != nil, found nil")
	}
	badSpeed = [][]int{
		[]int{0, 1, 1, 1},
		[]int{1, 0, 1, 1},
		[]int{1, 1, 0, 1, 0},
		[]int{1, 1, 1, 0},
	}
	_, err = New(cap, badSpeed)
	if err == nil {
		t.Errorf("expected err != nil, found nil")
	}
}

func TestNodeHost(t *testing.T) {
	var n Node

	n.freeCpus = 4
	if n.Host(5) {
		t.Errorf("expected n.Host(5) = fail, found success")
	}
	if n.freeCpus != 4 {
		t.Errorf("expected n.freeCpus = 4, found %d", n.freeCpus)
	}

	if !n.Host(2) {
		t.Errorf("expected n.Host(2) = true, found false")
	}
	if n.freeCpus != 2 {
		t.Errorf("expected n.freeCpus = 2, found %d", n.freeCpus)
	}
}

func TestDCHost(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 2},
		[2]int{2, 1},
	}
	speed := [][]int{
		[]int{0, 1},
		[]int{1, 0},
	}
	topo, err := New(cap, speed)
	if err != nil {
		t.Errorf("expected err = nil, found %v", err)
	}
	dc1 := topo.DataCenters[0]
	n, success := dc1.Host(2)
	if !success {
		t.Errorf("expected dc1.Host(2) = true, found %v", success)
	}
	if n != dc1.nodes[0] {
		t.Errorf("expected node = dcl.nodes[0], found %v", n)
	}
	if free := dc1.nodes[0].freeCpus; free != 0 {
		t.Errorf("expected dc1.nodes1.freeCpus = 0, found %d", free)
	}

	dc2 := topo.DataCenters[1]
	if _, success := dc2.Host(2); success {
		t.Errorf("expected dc2.Host(2) = false, found %v", success)
	}

	dc2.nodes[0].freeCpus = 0
	if n, success = dc2.Host(1); n != dc2.nodes[1] || !success {
		t.Errorf("expected dc2.Host(1) = dc2.node1, true, found %v, %v", n, success)
	}
}
