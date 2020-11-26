package scheduler

import (
	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/topology"
	"github.com/google/go-cmp/cmp"
	"testing"
)

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

	if len(scheduler.heap) != 2 {
		t.Fatalf("error adding jobs, expected 2 added, found %v", len(scheduler.heap))
	}
	if !cmp.Equal(job2, *scheduler.heap[0]) {
		t.Errorf("error adding job, expected heap[0]=%v, found %v", job2, scheduler.heap[0])
	}

	events := scheduler.Schedule(0)
	if len(events) != 2 {
		t.Fatalf("error scheduling jobs, expected 2 scheduled, found %v", len(events))
	}
	ev1 := events[0].(*topology.Node)
	if ev1.Time() != 20 {
		t.Errorf("error scheduling jobs, expected task ending at 20, found %v", ev1)
	}
	if node := topo.DataCenters[1].Get(0); ev1 != node {
		t.Errorf("error scheduling jobs, expected task in node %v, found at %v", node, ev1)
	}
	ev2 := events[1].(*topology.Node)
	if ev2.Time() != 100 {
		t.Errorf("error scheduling jobs, expected task ending at 20, found %v", ev1)
	}
	if node := topo.DataCenters[0].Get(0); ev2 != node {
		t.Errorf("error scheduling jobs, expected task in node %v, found at %v", node, ev2)
	}
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

	if len(scheduler.heap) != 2 {
		t.Fatalf("error adding jobs, expected 2 added, found %v", len(scheduler.heap))
	}
	if !cmp.Equal(job1, *scheduler.heap[0]) {
		t.Errorf("error adding job, expected heap[0]=%v, found %v", job1, scheduler.heap[0])
	}

	events := scheduler.Schedule(0)
	if len(events) != 2 {
		t.Fatalf("error scheduling jobs, expected 2 scheduled, found %v", len(events))
	}
	ev1 := events[0].(*topology.Node)
	if ev1.Time() != 30 {
		t.Errorf("error scheduling jobs, expected task ending at 20, found %v", ev1)
	}
	if node := topo.DataCenters[0].Get(0); ev1 != node {
		t.Errorf("error scheduling jobs, expected task in node %v, found at %v", node, ev1)
	}
	ev2 := events[1].(*topology.Node)
	if ev2.Time() != 20 {
		t.Errorf("error scheduling jobs, expected task ending at 20, found %v", ev1)
	}
	if node := topo.DataCenters[1].Get(0); ev2 != node {
		t.Errorf("error scheduling jobs, expected task in node %v, found at %v", node, ev2)
	}
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

	if scheduler.heap.Len() != 2 {
		t.Fatalf("error adding jobs, expected 2 added, found %v", scheduler.heap.Len())
	}
	scheduler.Update(0)
	if !cmp.Equal(job2, scheduler.heap.jobPile[0].Job) {
		t.Errorf("error adding job, expected heap[0]=%v, found %v", job2, scheduler.heap.jobPile[0])
	}

	events := scheduler.Schedule(0)
	if len(events) != 2 {
		t.Fatalf("error scheduling jobs, expected 2 scheduled, found %v", len(events))
	}
	ev1 := events[0].(*topology.Node)
	if ev1.Time() != 20 {
		t.Errorf("error scheduling jobs, expected task ending at 20, found %v", ev1)
	}
	if node := topo.DataCenters[1].Get(0); ev1 != node {
		t.Errorf("error scheduling jobs, expected task in node %v, found at %v", node, ev1)
	}
	ev2 := events[1].(*topology.Node)
	if ev2.Time() != 100 {
		t.Errorf("error scheduling jobs, expected task ending at 20, found %v", ev1)
	}
	if node := topo.DataCenters[0].Get(0); ev2 != node {
		t.Errorf("error scheduling jobs, expected task in node %v, found at %v", node, ev2)
	}
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

	if scheduler.heap.Len() != 2 {
		t.Fatalf("error adding jobs, expected 2 added, found %v", scheduler.heap.Len())
	}
	scheduler.Update(0)
	if !cmp.Equal(job2, scheduler.heap.jobPile[0].Job) {
		t.Errorf("error adding job, expected heap[0]=%v, found %v", job2, scheduler.heap.jobPile[0])
	}

	events := scheduler.Schedule(0)
	if len(events) != 2 {
		t.Fatalf("error scheduling jobs, expected 2 scheduled, found %v", len(events))
	}
	ev1 := events[0].(*topology.Node)
	if time := ev1.Time(); time != 20 {
		t.Errorf("error scheduling jobs, expected task ending at 20, found %v for %v", time, ev1)
	}
	if node := topo.DataCenters[1].Get(0); ev1 != node {
		t.Errorf("error scheduling jobs, expected task in node %v, found at %v", node, ev1)
	}
	ev2 := events[1].(*topology.Node)
	if time := ev2.Time(); time != 21 { // 1s for transferring file
		t.Errorf("error scheduling jobs, expected task ending at 30, found %v for %v", time, ev2)
	}
	if node := topo.DataCenters[0].Get(0); ev2 != node {
		t.Errorf("error scheduling jobs, expected task in node %v, found at %v", node, ev2)
	}
	if scheduler.heap.Len() != 1 {
		t.Fatalf("error scheduling jobs, expected job heap to have size 0, found %v", scheduler.heap.Len())
	}
}
