package simulator

import (
	"container/heap"

	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/log"
	"github.com/dsfalves/gdsim/scheduler"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
)

var logger log.Context

func init() {
	logger = log.New("simulator")
}

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
	eventHeap event.EventHeap
}

func (scheduling WindowScheduling) Time() uint64 {
	return scheduling.When
}

func (scheduling WindowScheduling) Process() []event.Event {
	jobEvents := scheduling.Scheduler.Schedule(scheduling.When)
	if scheduling.eventHeap.Len() > 0 {
		when := scheduling.When + scheduling.Window
		if nextEvent := scheduling.eventHeap[0].Time(); when <= nextEvent {
			when = nextEvent - nextEvent%scheduling.Window + scheduling.Window
		}
		next := WindowScheduling{
			When:      when,
			Window:    scheduling.Window,
			Scheduler: scheduling.Scheduler,
			eventHeap: scheduling.eventHeap,
		}
		jobEvents = append(jobEvents, next)
	}
	return jobEvents
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
		eventHeap: sim.Heap,
	})

	return sim
}

func (simulation Simulation) Run(window float64) ([]Result, error) {
	// Create JobArrival Events
	// While there are events to process
	// Process next event
	for len(simulation.Heap) > 1 {
		e := heap.Pop(&simulation.Heap).(event.Event)
		logger.Infof("simulator popped %p", &e)
		logger.Infof("%d events remaining:", len(simulation.Heap))
		for _, new_event := range e.Process() {
			logger.Infof("simulator adding %p", &new_event)
			heap.Push(&simulation.Heap, new_event)
		}
	}
	return nil, nil
}

type Result struct {
	Job       *job.Job
	Durations []uint64
}
