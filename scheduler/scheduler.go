package scheduler

import (
	"container/heap"
	"fmt"
	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
	"log"
	"sort"
)

type jobHeap []*job.Job

func rpt(j job.Job) uint64 {
	var total uint64
	for _, task := range j.Tasks {
		total += task.Duration
	}
	return total
}

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return rpt(*h[i]) < rpt(*h[j]) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*job.Job))
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type GlobalSRPTScheduler struct {
	heap     jobHeap
	topology topology.Topology
	jobs     map[string]*job.Job
}

func NewGRPTS(t topology.Topology) GlobalSRPTScheduler {
	scheduler := GlobalSRPTScheduler{
		topology: t,
		jobs:     make(map[string]*job.Job),
	}
	heap.Init(&scheduler.heap)
	return scheduler
}

func (scheduler *GlobalSRPTScheduler) Add(j *job.Job) {
	sort.Slice(j.Tasks, func(i, k int) bool { return j.Tasks[i].Duration < j.Tasks[k].Duration })
	heap.Push(&scheduler.heap, j)
	scheduler.jobs[j.Id] = j
}

func transferTime(size uint64, t topology.Topology, from, to int) uint64 {
	if from == to {
		return 0
	}
	return size / t.Speeds[from][to]
}

func (scheduler GlobalSRPTScheduler) Results() map[string]*job.Job {
	return scheduler.jobs
}

type transferCenter struct {
	transferTime uint64
	dataCenter   *topology.DataCenter
}

func bestDCs(f file.File, t topology.Topology) []transferCenter {
	res := make([]transferCenter, len(t.DataCenters))

	for i := range t.DataCenters {
		res[i].dataCenter = t.DataCenters[i]
		res[i].transferTime = transferTime(f.Size, t, f.Locations[0], i)
		for k := 1; k < len(f.Locations); k++ {
			from := f.Locations[k]
			if transfer := transferTime(f.Size, t, from, i); transfer < res[i].transferTime {
				res[i].transferTime = transfer
			}
		}
	}
	sort.Slice(res, func(i, k int) bool { return res[i].transferTime < res[k].transferTime })
	return res
}

type taskEndEvent struct {
	start, duration uint64
	cpus            int
	host            *topology.Node
	where           string
	job             *job.Job
}

func (event taskEndEvent) Time() uint64 {
	return event.start + event.duration
}

func (event taskEndEvent) Process() []event.Event {
	event.host.Free(event.cpus)
	event.job.Scheduled = append(event.job.Scheduled, job.DoneTask{
		Start:    event.start,
		Duration: event.duration,
		Location: event.where,
	})
	log.Printf("added event to Scheduled - len(Scheduled) = %v\n", len(event.job.Scheduled))
	return nil
}

func (scheduler *GlobalSRPTScheduler) Schedule(now uint64) []event.Event {
	events := make([]event.Event, 0)
	for scheduler.heap.Len() > 0 {
		top := scheduler.heap[0]
		dcs := bestDCs(top.File, scheduler.topology)
		for len(top.Tasks) > 0 {
			hosted := false
			for i, dc := range dcs {
				if node, success := dc.dataCenter.Host(int(top.Cpus)); success {
					task := top.Tasks[len(top.Tasks)-1]
					top.Tasks = top.Tasks[:len(top.Tasks)-1]
					events = append(events, taskEndEvent{
						start:    dc.transferTime + now,
						duration: task.Duration,
						cpus:     int(top.Cpus),
						host:     node,
						where:    fmt.Sprint("DC%v", i),
					})
					hosted = true
					log.Printf("scheduling task %v\n", task)
					break
				}
			}
			if !hosted {
				return events
			}
			heap.Pop(&scheduler.heap)
		}
	}
	return events
}

type Scheduler interface {
	//Pop() *job.Task
	Add(t *job.Job)
	Schedule(now uint64) []event.Event
}

/*
type jobEvent struct {
	jobs []*job.Job
	time uint64
}

func (je *jobEvent) Time() uint64 {
	return je.time
}

func (je *jobEvent) Process(s *Scheduler) {
	for _, job := range je.jobs {
		s.js.Add(job)
	}
}

type EventManager struct {
	h  event.EventHeap
	js JobScheduler
}

func New(jobs []*job.Job, js JobScheduler) *EventManager {
	var s EventManager
	s.js = js

	return &s
}

func (s *EventManager) Run(jobs []*job.Job) {
	var h EventHeap
	for _, job := range jobs {
		h.Push(job)
	}
	for len(h) > 0 {
		next := h.Pop().(Event)
		next.Process(s)
		newEvents := s.js.Update()
		for _, e := range newEvents {
			h.Push(e)
		}
	}
}
*/
