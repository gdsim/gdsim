package simulator

import (
	"container/heap"
	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/scheduler"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
)

type JobArrival struct {
	Job       job.Job
	Scheduler scheduler.Scheduler
}

func (arrival JobArrival) Time() uint64 {
	return arrival.Job.Submission
}

func (arrival JobArrival) Process() []event.Event {
	arrival.Scheduler.Add(&arrival.Job)
	return nil
}

type WindowScheduling struct {
	When      uint64
	Window    uint64
	Scheduler scheduler.Scheduler
}

func (scheduling WindowScheduling) Time() uint64 {
	return scheduling.When
}

func (scheduling WindowScheduling) Process() []event.Event {
	jobEvents := scheduling.Scheduler.Schedule()
	next := WindowScheduling{
		When:   scheduling.When + scheduling.Window,
		Window: scheduling.Window,
	}
	return append(jobEvents, next)
}

type Simulation struct {
	Jobs      []job.Job
	Files     map[string]file.File
	Topo      *topology.Topology
	Heap      event.EventHeap
	Scheduler scheduler.Scheduler
}

func New(jobs []job.Job, files map[string]file.File, topo *topology.Topology, scheduler scheduler.Scheduler) *Simulation {
	sim := &Simulation{
		Jobs:      jobs,
		Files:     files,
		Topo:      topo,
		Scheduler: scheduler,
	}
	heap.Init(&sim.Heap)
	min := jobs[0].Submission
	for _, j := range jobs {
		heap.Push(&sim.Heap, JobArrival{
			Job:       j,
			Scheduler: scheduler,
		})
		if j.Submission < min {
			min = j.Submission
		}
	}
	heap.Push(&sim.Heap, WindowScheduling{
		When:      min + 1,
		Window:    5,
		Scheduler: scheduler,
	})

	return sim
}

func (simulation Simulation) Run(window float64) ([]Result, error) {
	// Create JobArrival Events
	// While there are events to process
	// Process next event
	for len(simulation.Heap) > 0 {
		e := heap.Pop(&simulation.Heap).(event.Event)
		e.Process()
	}
	return nil, nil
}

type Result struct {
	Job       *job.Job
	Durations []uint64
}
