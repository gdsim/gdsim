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

/*
type testJobScheduler struct {
	time           uint64
	procJobs       uint
	procTasks      uint
	remainingJobs  uint
	remainingTasks uint
}

func (tjs *testJobScheduler) Time() uint64 {
	return tjs.time
}

func TestRun(t *testing.T) {
	jobs := []job.Job{
		job.Job{
			Id:         "j1",
			Submission: 0,
			Cpus:       1,
			Tasks: []job.Task{
				job.Task{1},
				job.Task{2},
			},
			File: "f1",
		},
		job.Job{
			Id:         "j2",
			Submission: 0,
			Cpus:       1,
			Tasks: []job.Task{
				job.Task{1},
				job.Task{2},
			},
			File: "f2",
		},
	}
	topo, err := topology.New(
		[][2]int{
			[2]int{1, 2},
		},
		[][]int{
			[]int{1000},
		},
	)
	if err != nil {
		t.Fatalf("error when creating topology: %v", err)
	}
	files := map[string]file.File{
		"f1": file.File{
			Size:      10,
			Locations: []*topology.DataCenter{topo.DataCenters[0]},
		},
		"f2": file.File{
			Size:      10,
			Locations: []*topology.DataCenter{topo.DataCenters[0]},
		},
	}
	var tjs testJobScheduler

	tjs.time = 0
}
*/
