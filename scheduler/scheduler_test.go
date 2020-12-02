package scheduler

import (
	"container/heap"
	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
	"github.com/google/go-cmp/cmp"
	"testing"
)

type schedulerHeap interface {
	Top() *job.Job
	heap.Interface
}

func checkHeap(t *testing.T, heap schedulerHeap, length int, j *job.Job) {
	if heap.Len() != 2 {
		t.Fatalf("error adding jobs, expected %d added, found %v", length, heap.Len())
	}
	top := heap.Top()
	if !cmp.Equal(*j, *top) {
		t.Errorf("error adding job, expected heap[0]=%v, found %v", j, top)
	}
}

type expected struct {
	time uint64
	node *topology.Node
}

func checkEvents(t *testing.T, events []event.Event, answers []expected) {
	if len(events) != len(answers) {
		t.Fatalf("error scheduling jobs, expected %v scheduled, found %v", len(answers), len(events))
	}
	for i, e := range answers {
		ev, ok := events[i].(*topology.Node)
		if !ok {
			t.Fatalf("error in return of Schedule: %v is not *topology.Node", ev)
		}
		if time := ev.Time(); time != e.time {
			t.Errorf("error scheduling jobs, expected task ending at %d, found %d", e.time, time)
		}
	}
}

func TestGSRPT(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 1},
		[2]int{1, 1},
	}
	speeds := [][]uint64{
		[]uint64{0, 10},
		[]uint64{10, 0},
	}
	topo, err := topology.New(cap, speeds)
	if err != nil {
		t.Fatalf("failure to setup test: %v", err)
	}
	file1 := file.File{
		Size:      100,
		Locations: []int{0},
	}
	file2 := file.File{
		Size:      200,
		Locations: []int{1},
	}
	job1 := job.Job{
		Id:         "job1",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{100},
		},
		File: file1,
	}
	job2 := job.Job{
		Id:         "job2",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{20},
		},
		File: file2,
	}

	scheduler := NewGRPTS(*topo)

	scheduler.Add(&job1)
	scheduler.Add(&job2)

	checkHeap(t, &scheduler.heap, 2, &job2)

	events := scheduler.Schedule(0)
	answers := []expected{
		expected{
			time: 20,
			node: topo.DataCenters[1].Get(0),
		},
		expected{
			time: 100,
			node: topo.DataCenters[0].Get(0),
		},
	}
	checkEvents(t, events, answers)
	if len(scheduler.heap) != 0 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", len(scheduler.heap))
	}
}

func TestGSRPT2(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 1},
		[2]int{1, 1},
	}
	speeds := [][]uint64{
		[]uint64{0, 10},
		[]uint64{10, 0},
	}
	topo, err := topology.New(cap, speeds)
	if err != nil {
		t.Fatalf("failure to setup test: %v", err)
	}
	file1 := file.File{
		Size:      100,
		Locations: []int{0},
	}
	file2 := file.File{
		Size:      200,
		Locations: []int{1},
	}
	job1 := job.Job{
		Id:         "job1",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{30},
		},
		File: file1,
	}
	job2 := job.Job{
		Id:         "job2",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{20},
			job.Task{20},
		},
		File: file2,
	}

	scheduler := NewGRPTS(*topo)

	scheduler.Add(&job1)
	scheduler.Add(&job2)

	checkHeap(t, &scheduler.heap, 2, &job1)

	events := scheduler.Schedule(0)
	answers := []expected{
		expected{
			time: 30,
			node: topo.DataCenters[0].Get(0),
		},
		expected{
			time: 20,
			node: topo.DataCenters[1].Get(0),
		},
	}
	checkEvents(t, events, answers)
	if len(scheduler.heap) != 1 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", len(scheduler.heap))
	}
}

func TestGeoDis(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 1},
		[2]int{1, 1},
	}
	speeds := [][]uint64{
		[]uint64{0, 10},
		[]uint64{10, 0},
	}
	topo, err := topology.New(cap, speeds)
	if err != nil {
		t.Fatalf("failure to setup test: %v", err)
	}
	file1 := file.File{
		Size:      100,
		Locations: []int{0},
	}
	file2 := file.File{
		Size:      200,
		Locations: []int{1},
	}
	job1 := job.Job{
		Id:         "job1",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{100},
		},
		File: file1,
	}
	job2 := job.Job{
		Id:         "job2",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{20},
		},
		File: file2,
	}

	scheduler := NewGeoDis(*topo)

	scheduler.Add(&job1)
	scheduler.Add(&job2)

	scheduler.Update(0)
	checkHeap(t, &scheduler.heap, 2, &job2)

	events := scheduler.Schedule(0)
	answers := []expected{
		expected{
			time: 20,
			node: topo.DataCenters[1].Get(0),
		},
		expected{
			time: 100,
			node: topo.DataCenters[0].Get(0),
		},
	}
	checkEvents(t, events, answers)
	if scheduler.heap.Len() != 0 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", scheduler.heap.Len())
	}
}

func TestGeoDis2(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 1},
		[2]int{1, 1},
	}
	speeds := [][]uint64{
		[]uint64{0, 10},
		[]uint64{10, 0},
	}
	topo, err := topology.New(cap, speeds)
	if err != nil {
		t.Fatalf("failure to setup test: %v", err)
	}
	file1 := file.File{
		Size:      20,
		Locations: []int{0},
	}
	file2 := file.File{
		Size:      10,
		Locations: []int{1},
	}
	job1 := job.Job{
		Id:         "job1",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{35},
		},
		File: file1,
	}
	job2 := job.Job{
		Id:         "job2",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{20},
			job.Task{20},
		},
		File: file2,
	}

	scheduler := NewGeoDis(*topo)

	scheduler.Add(&job1)
	scheduler.Add(&job2)

	scheduler.Update(0)
	checkHeap(t, &scheduler.heap, 2, &job2)

	events := scheduler.Schedule(0)
	answers := []expected{
		expected{
			time: 20,
			node: topo.DataCenters[1].Get(0),
		},
		expected{
			time: 21,
			node: topo.DataCenters[0].Get(0),
		},
	}
	checkEvents(t, events, answers)

	if scheduler.heap.Len() != 1 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", scheduler.heap.Len())
	}
}

func TestGeoDis3(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 1},
		[2]int{1, 1},
	}
	speeds := [][]uint64{
		[]uint64{0, 10},
		[]uint64{10, 0},
	}
	topo, err := topology.New(cap, speeds)
	if err != nil {
		t.Fatalf("failure to setup test: %v", err)
	}
	file1 := file.File{
		Size:      20,
		Locations: []int{0},
	}
	file2 := file.File{
		Size:      200,
		Locations: []int{1},
	}
	job1 := job.Job{
		Id:         "job1",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{35},
		},
		File: file1,
	}
	job2 := job.Job{
		Id:         "job2",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{20},
			job.Task{20},
		},
		File: file2,
	}

	scheduler := NewGeoDis(*topo)

	scheduler.Add(&job1)
	scheduler.Add(&job2)

	scheduler.Update(0)
	checkHeap(t, &scheduler.heap, 2, &job1)

	events := scheduler.Schedule(0)
	answers := []expected{
		expected{
			time: 35,
			node: topo.DataCenters[0].Get(0),
		},
		expected{
			time: 20,
			node: topo.DataCenters[1].Get(0),
		},
	}
	checkEvents(t, events, answers)

	if scheduler.heap.Len() != 1 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", scheduler.heap.Len())
	}
}

func TestSwag(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 1},
		[2]int{1, 1},
	}
	speeds := [][]uint64{
		[]uint64{0, 10},
		[]uint64{10, 0},
	}
	topo, err := topology.New(cap, speeds)
	if err != nil {
		t.Fatalf("failure to setup test: %v", err)
	}
	file1 := file.File{
		Size:      100,
		Locations: []int{0},
	}
	file2 := file.File{
		Size:      200,
		Locations: []int{1},
	}
	job1 := job.Job{
		Id:         "job1",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{100},
		},
		File: file1,
	}
	job2 := job.Job{
		Id:         "job2",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{20},
		},
		File: file2,
	}

	scheduler := NewSwag(*topo)

	scheduler.Add(&job1)
	scheduler.Add(&job2)

	scheduler.Update(0)
	checkHeap(t, &scheduler.heap, 2, &job2)

	events := scheduler.Schedule(0)
	answers := []expected{
		expected{
			time: 20,
			node: topo.DataCenters[1].Get(0),
		},
		expected{
			time: 100,
			node: topo.DataCenters[0].Get(0),
		},
	}
	checkEvents(t, events, answers)
	if scheduler.heap.Len() != 0 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", scheduler.heap.Len())
	}
}

func TestSwag2(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 1},
		[2]int{1, 1},
	}
	speeds := [][]uint64{
		[]uint64{0, 10},
		[]uint64{10, 0},
	}
	topo, err := topology.New(cap, speeds)
	if err != nil {
		t.Fatalf("failure to setup test: %v", err)
	}
	file1 := file.File{
		Size:      20,
		Locations: []int{0},
	}
	file2 := file.File{
		Size:      10,
		Locations: []int{1},
	}
	job1 := job.Job{
		Id:         "job1",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{35},
		},
		File: file1,
	}
	job2 := job.Job{
		Id:         "job2",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{20},
			job.Task{20},
		},
		File: file2,
	}

	scheduler := NewSwag(*topo)

	scheduler.Add(&job1)
	scheduler.Add(&job2)

	scheduler.Update(0)
	checkHeap(t, &scheduler.heap, 2, &job1)

	events := scheduler.Schedule(0)
	answers := []expected{
		expected{
			time: 35,
			node: topo.DataCenters[0].Get(0),
		},
		expected{
			time: 20,
			node: topo.DataCenters[1].Get(0),
		},
	}
	checkEvents(t, events, answers)

	if scheduler.heap.Len() != 1 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", scheduler.heap.Len())
	}
}

func TestSwag3(t *testing.T) {
	cap := [][2]int{
		[2]int{1, 1},
		[2]int{1, 1},
	}
	speeds := [][]uint64{
		[]uint64{0, 10},
		[]uint64{10, 0},
	}
	topo, err := topology.New(cap, speeds)
	if err != nil {
		t.Fatalf("failure to setup test: %v", err)
	}
	file1 := file.File{
		Size:      20,
		Locations: []int{0},
	}
	file2 := file.File{
		Size:      200,
		Locations: []int{1},
	}
	job1 := job.Job{
		Id:         "job1",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{35},
		},
		File: file1,
	}
	job2 := job.Job{
		Id:         "job2",
		Submission: 0,
		Cpus:       1,
		Tasks: []job.Task{
			job.Task{20},
			job.Task{20},
		},
		File: file2,
	}

	scheduler := NewSwag(*topo)

	scheduler.Add(&job1)
	scheduler.Add(&job2)

	scheduler.Update(0)
	checkHeap(t, &scheduler.heap, 2, &job1)

	events := scheduler.Schedule(0)
	answers := []expected{
		expected{
			time: 35,
			node: topo.DataCenters[0].Get(0),
		},
		expected{
			time: 20,
			node: topo.DataCenters[1].Get(0),
		},
	}
	checkEvents(t, events, answers)

	if scheduler.heap.Len() != 1 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", scheduler.heap.Len())
	}
}
