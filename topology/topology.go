package topology

import (
	"container/heap"
	"fmt"
	"io"

	"github.com/dsfalves/gdsim/log"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/google/go-cmp/cmp"
)

var logger log.Context

func init() {
	logger = log.New("topology")
}

type RunningTask interface {
	End() uint64
	Cpus() int
}

type taskHeap []RunningTask

func (h taskHeap) Len() int           { return len(h) }
func (h taskHeap) Less(i, j int) bool { return h[i].End() < h[j].End() }
func (h taskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *taskHeap) Push(x interface{}) {
	*h = append(*h, x.(RunningTask))
}

func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Node struct {
	Location int
	freeCpus int
	capacity int
	heap     taskHeap
}

type Data interface {
	Id() string
	Size() uint64
}

type Container interface {
	Add(id string, data Data)
	Has(id string) bool
	Find(id string) Data
	Pop(id string) Data
}

type DataCenter struct {
	id        int
	nodes     []*Node
	Container Container
}

/*
Returns how many jobs requiring *cost* CPU slots a data center can host at most.
*/
func (dc DataCenter) JobCapacity(cost int) int {
	return (dc.nodes[0].capacity / cost) * len(dc.nodes)
}

/*
Returns how many jobs requiring *cost* CPU slots a data center can currently host
given available free space.
*/
func (dc DataCenter) JobAvailability(cost int) (free int) {
	for _, n := range dc.nodes {
		free += n.freeCpus / cost
	}
	return free
}

/*
Returns a pointer to the corresponding node.
This is meant only to be used in tests.
*/
func (dc DataCenter) Get(n int) *Node {
	if n < len(dc.nodes) {
		return dc.nodes[n]
	}
	return nil
}

func (dc DataCenter) ExpectedEndings() []uint64 {
	endings := make([]uint64, 0)
	for _, node := range dc.nodes {
		for _, task := range node.heap {
			endings = append(endings, task.End())
		}
	}
	return endings
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
		topo.DataCenters[i] = &DataCenter{
			id:    i,
			nodes: n,
		}
		for k := range topo.DataCenters[i].nodes {
			topo.DataCenters[i].nodes[k] = NewNode(nCpus, i)
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

	n, err := fmt.Fscan(topoInfo, &size)
	if err != nil {
		return nil, fmt.Errorf("failure to read topology: size error: %v", err)
	} else if n != 1 {
		return nil, fmt.Errorf("failure to read topology: size error: missing size")
	}

	capacity := make([][2]int, size)
	for i := 0; i < size; i++ {
		n, err := fmt.Fscan(topoInfo, &capacity[i][0], &capacity[i][1])
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
			n, err := fmt.Fscan(topoInfo, &speeds[i][k])
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

func NewNode(capacity int, location int) *Node {
	var n Node
	n.freeCpus = capacity
	n.capacity = capacity
	n.Location = location
	heap.Init(&n.heap)
	return &n
}

func (n *Node) Host(task RunningTask) bool {
	if task.Cpus() <= n.freeCpus {
		n.freeCpus -= task.Cpus()
		n.heap.Push(task)
		return true
	}
	logger.Debugf("node failed to host task with %d CPUS: available capacity is %d", task.Cpus(), n.freeCpus)
	return false
}

func (n *Node) Free(cpus int) {
	n.freeCpus += cpus
}

func (n *Node) QueueLen() int {
	return n.heap.Len()
}

func (n *Node) Process() []event.Event {
	logger.Debugf("%p.Process()", n)
	t := heap.Pop(&n.heap).(RunningTask)
	n.Free(t.Cpus())
	if n.heap.Len() > 0 {
		logger.Infof("keeping node %p: %d tasks remaining", n, n.heap.Len())
		return []event.Event{n}
	}
	logger.Infof("removing node %p", n)
	return nil
}

func (n *Node) Time() uint64 {
	logger.Debugf("%p.Time()", n)
	if len(n.heap) == 0 {
		logger.Fatalf("node %p has no tasks", n)
		return 0
	}
	return n.heap[0].End()
}

func (dc *DataCenter) Host(task RunningTask) (*Node, bool) {
	for _, n := range dc.nodes {
		if n.Host(task) {
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
