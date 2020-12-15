package scheduler

import (
	"container/heap"
	"sort"

	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
)

type GlobalSRPTScheduler struct {
	heap     jobHeap
	topology topology.Topology
	jobs     map[string]*job.Job
}

func NewGRPTS(t topology.Topology) *GlobalSRPTScheduler {
	scheduler := &GlobalSRPTScheduler{
		topology: t,
		jobs:     make(map[string]*job.Job),
	}
	heap.Init(&scheduler.heap)
	return scheduler
}

func (scheduler *GlobalSRPTScheduler) Add(j *job.Job) {
	logger.Debugf("%p.Add(%p)", scheduler, j)
	sort.Slice(j.Tasks, func(i, k int) bool { return j.Tasks[i].Duration < j.Tasks[k].Duration })
	heap.Push(&scheduler.heap, j)
	scheduler.jobs[j.Id] = j
}

func (scheduler *GlobalSRPTScheduler) Schedule(now uint64) []event.Event {
	logger.Debugf("%p.Schedule(%v)", scheduler, now)
	events := make([]event.Event, 0)
	for scheduler.heap.Len() > 0 {
		top := scheduler.heap[0]
		dcs := fullBestDcs(top.File, scheduler.topology, int(top.Cpus))
		for len(top.Tasks) > 0 {
			hosted := false
			for _, dc := range dcs {
				task := top.Tasks[len(top.Tasks)-1]
				taskEnd := taskEndEvent{
					start:    dc.transferTime + now,
					duration: task.Duration,
					cpus:     int(top.Cpus),
					job:      top,
				}
				if node, success := dc.dataCenter.Host(taskEnd); success {
					top.Tasks = top.Tasks[:len(top.Tasks)-1]
					taskEnd.where = node.Location
					taskEnd.Process()
					hosted = true
					logger.Infof("scheduling task %v for job %v", task, top.Id)
					if node.QueueLen() == 1 {
						events = append(events, node)
					}
					break
				} else {
					logger.Infof("failed scheduling task for job %p", top.Id)
				}
			}
			if !hosted {
				return events
			}
		}
		heap.Pop(&scheduler.heap)
	}
	return events
}

func (scheduler GlobalSRPTScheduler) Results() map[string]*job.Job {
	return scheduler.jobs
}
