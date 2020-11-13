package topology

import (
	"github.com/google/go-cmp/cmp"
	"strings"
	"testing"
)

func TestDataCenterEqual(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 2},
		[2]int{1, 2},
		[2]int{2, 1},
		[2]int{2, 1},
	}
	speed := [][]uint64{
		[]uint64{0, 1, 1, 1},
		[]uint64{1, 0, 1, 1},
		[]uint64{1, 1, 0, 1},
		[]uint64{1, 1, 1, 0},
	}
	topo, err := New(cap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}
	if topo.DataCenters[0].Equal(*topo.DataCenters[2]) {
		t.Errorf("expected %v.Equal(%v) == false, found true", topo.DataCenters[0], topo.DataCenters[2])
	}
	if !topo.DataCenters[0].Equal(*topo.DataCenters[1]) {
		t.Errorf("expected %v.Equal(%v) == true, found false", topo.DataCenters[0], topo.DataCenters[1])
	}
	if !topo.DataCenters[2].Equal(*topo.DataCenters[3]) {
		t.Errorf("expected %v.Equal(%v) == true, found false", topo.DataCenters[2], topo.DataCenters[3])
	}
}

func TestNew(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 2},
		[2]int{2, 1},
		[2]int{3, 5},
		[2]int{1, 3},
	}
	speed := [][]uint64{
		[]uint64{0, 1, 1, 1},
		[]uint64{1, 0, 1, 1},
		[]uint64{1, 1, 0, 1},
		[]uint64{1, 1, 1, 0},
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
	if !cmp.Equal(speed, topo.Speeds) {
		t.Errorf("expected topo.Speeds = %v, found %v", speed, topo.Speeds)
	}

	badSpeed := [][]uint64{
		[]uint64{0, 1, 1, 1},
		[]uint64{1, 0, 1, 1},
		[]uint64{1, 1, 0, 1},
		[]uint64{1, 1, 1, 0},
		[]uint64{1, 1, 1, 0},
	}
	_, err = New(cap, badSpeed)
	if err == nil {
		t.Errorf("expected err != nil, found nil")
	}
	badSpeed = [][]uint64{
		[]uint64{0, 1, 1, 1},
		[]uint64{1, 0, 1, 1},
		[]uint64{1, 1, 0, 1, 0},
		[]uint64{1, 1, 1, 0},
	}
	_, err = New(cap, badSpeed)
	if err == nil {
		t.Errorf("expected err != nil, found nil")
	}
}

type sampleTask struct {
	end  uint64
	cpus int
}

func (t sampleTask) End() uint64 { return t.end }
func (t sampleTask) Cpus() int   { return t.cpus }

func TestNodeHost(t *testing.T) {
	t1 := sampleTask{
		end:  10,
		cpus: 5,
	}
	t2 := sampleTask{
		end:  20,
		cpus: 2,
	}

	n := NewNode(4)
	if n.Host(t1) {
		t.Errorf("expected n.Host(5) = fail, found success")
	}
	if n.freeCpus != 4 {
		t.Errorf("expected n.freeCpus = 4, found %d", n.freeCpus)
	}
	if n.heap.Len() != 0 {
		t.Errorf("expected n.heap.Len() = 0, found %d", n.heap.Len())
	}

	if !n.Host(t2) {
		t.Errorf("expected n.Host(2) = true, found false")
	}
	if n.freeCpus != 2 {
		t.Errorf("expected n.freeCpus = 2, found %d", n.freeCpus)
	}
	if n.heap.Len() != 1 {
		t.Errorf("expected n.heap.Len() = 0, found %d", n.heap.Len())
	}
}

func TestDCHost(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 2},
		[2]int{2, 1},
	}
	speed := [][]uint64{
		[]uint64{0, 1},
		[]uint64{1, 0},
	}
	t1 := sampleTask{
		end:  10,
		cpus: 2,
	}
	t2 := sampleTask{
		end:  20,
		cpus: 1,
	}

	topo, err := New(cap, speed)
	if err != nil {
		t.Errorf("expected err = nil, found %v", err)
	}
	dc1 := topo.DataCenters[0]
	n, success := dc1.Host(t1)
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
	if _, success := dc2.Host(t1); success {
		t.Errorf("expected dc2.Host(2) = false, found %v", success)
	}

	dc2.nodes[0].freeCpus = 0
	if n, success = dc2.Host(t2); n != dc2.nodes[1] || !success {
		t.Errorf("expected dc2.Host(1) = dc2.node1, true, found %v, %v", n, success)
	}
}

func TestFree(t *testing.T) {
	n := NewNode(5)

	n.Free(2)
	if n.freeCpus != 7 {
		t.Errorf("expected n.freeCpus = %v, found %v", 7, n.freeCpus)
	}
}

func testDC(t *testing.T, size, cpus int, dc *DataCenter) {
	numNodes := len(dc.nodes)
	if numNodes != size {
		t.Errorf("wrong number of data centers created: expected %v, found %v", size, numNodes)
	}

	for i, node := range dc.nodes {
		if node.freeCpus != cpus {
			t.Errorf("wrong number of free cpus on node[%v]: expected %v, found %v", i, cpus, node.freeCpus)
		}
	}
}

func TestLoad(t *testing.T) {
	sample := "3\n2 1\n3 2\n4 3\n1000 99 200\n99 1000 500\n200 500 1000\n"
	reader := strings.NewReader(sample)
	topo, err := Load(reader)
	if err != nil {
		t.Fatalf("error '%v' while processing topology '%v', expected nil", err, sample)
	}

	numDC := len(topo.DataCenters)
	if numDC != 3 {
		t.Errorf("error while loading topology '%v': expected %v, found %v", sample, numDC, 2)
	}
	testDC(t, 2, 1, topo.DataCenters[0])
	testDC(t, 3, 2, topo.DataCenters[1])
	testDC(t, 4, 3, topo.DataCenters[2])

	speeds := [][]uint64{
		[]uint64{1000, 99, 200},
		[]uint64{99, 1000, 500},
		[]uint64{200, 500, 1000},
	}
	if !cmp.Equal(speeds, topo.Speeds) {
		t.Errorf("error while loading topology '%v': expected dc.Speeds = %v, found %v", sample, speeds, topo.Speeds)
	}
}

func TestTopologyEqual(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 2},
		[2]int{2, 1},
	}
	speed := [][]uint64{
		[]uint64{0, 1},
		[]uint64{1, 0},
	}
	fakeCap := [][2]int{
		[2]int{1, 2},
		[2]int{2, 2},
	}
	fakeSpeed := [][]uint64{
		[]uint64{0, 2},
		[]uint64{1, 0},
	}

	topo1, err := New(cap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}
	topo2, err := New(cap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}
	topo3, err := New(fakeCap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}
	topo4, err := New(cap, fakeSpeed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}

	if !topo1.Equal(*topo2) {
		t.Errorf("found %v.Equal(%v) == false, expected true", topo1, topo2)
	}
	if topo1.Equal(*topo3) {
		t.Errorf("found %v.Equal(%v) == false, expected true", topo1, topo3)
	}
	if topo1.Equal(*topo4) {
		t.Errorf("found %v.Equal(%v) == false, expected true", topo1, topo4)
	}
}

// TODO: add tests for errors
