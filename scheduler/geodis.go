package scheduler

import (
	"container/heap"
	"sort"

	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
)

type scheduledTask struct {
	duration   uint64
	dataCenter *topology.DataCenter
}

type makespanJob struct {
	job.Job
	tasks    []scheduledTask
	makespan uint64
	bestDcs  func(file.File, topology.Topology, int) []transferCenter
}

type endQueue []uint64

func (queue endQueue) Len() int            { return len(queue) }
func (queue endQueue) Less(i, j int) bool  { return queue[i] < queue[j] }
func (queue endQueue) Swap(i, j int)       { queue[i], queue[j] = queue[j], queue[i] }
func (queue *endQueue) Push(x interface{}) { *queue = append((*queue), x.(uint64)) }
func (queue *endQueue) Pop() interface{} {
	x := (*queue)[0]
	*queue = (*queue)[1:]
	return x
}

type lightDc struct {
	free, total  int
	transferTime uint64
	now          uint64
	endTimes     endQueue
}

func (dc lightDc) ending() uint64 {
	if len(dc.endTimes) > 0 {
		return dc.endTimes[0] + dc.transferTime
	}
	return dc.now + dc.transferTime
}

type dcHeap []lightDc

func (heap dcHeap) Len() int { return len(heap) }
func (heap dcHeap) Less(i, j int) bool {
	end1 := heap[i].ending() + heap[i].transferTime
	end2 := heap[j].ending() + heap[j].transferTime
	return end1 < end2
}
func (heap dcHeap) Swap(i, j int)       { heap[i], heap[j] = heap[j], heap[i] }
func (heap *dcHeap) Push(x interface{}) { *heap = append(*heap, x.(lightDc)) }
func (heap *dcHeap) Pop() interface{} {
	x := (*heap)[0]
	*heap = (*heap)[1:]
	return x
}

func lightCopy(tcs []transferCenter, now uint64) []lightDc {
	fakeTcs := make([]lightDc, 0, len(tcs))

	for _, tc := range tcs {
		var fakeTc lightDc
		fakeTc.total = tc.capacity
		if fakeTc.total == 0 {
			continue
		}
		fakeTc.free = tc.freeJobSlots
		fakeTc.transferTime = tc.transferTime
		fakeTc.endTimes = make(endQueue, 0, fakeTc.total)
		fakeTc.now = now
		busy := fakeTc.total - fakeTc.free
		endings := tc.dataCenter.ExpectedEndings()
		excess := busy - len(endings)
		for _, ending := range endings {
			heap.Push(&fakeTc.endTimes, ending)
		}
		for k := 0; k < excess; k++ {
			heap.Pop(&fakeTc.endTimes)
		}
		fakeTcs = append(fakeTcs, fakeTc)

	}

	return fakeTcs
}

func (tc *lightDc) fakeHost(task job.Task, now uint64) uint64 {
	var time uint64 = task.Duration + tc.transferTime + now
	if tc.free > 0 {
		tc.free--
		heap.Push(&tc.endTimes, time)
	} else {
		now = tc.endTimes[0]
		time = task.Duration + tc.transferTime + now
		tc.endTimes[0] = time
		heap.Fix(&tc.endTimes, 0)
	}

	return time
}

func (j *makespanJob) updateMakespan(t topology.Topology, now uint64) {
	tc := j.bestDcs(j.File, t, int(j.Cpus))
	var fakeTcs dcHeap = lightCopy(tc, now)
	heap.Init(&fakeTcs)
	j.makespan = 0

	for _, task := range j.Tasks {
		endTime := fakeTcs[0].fakeHost(task, now)
		if endTime > j.makespan {
			j.makespan = endTime
		}
		heap.Fix(&fakeTcs, 0)
	}
}

type makespanHeap struct {
	jobPile []*makespanJob
	topo    topology.Topology
}

func (h makespanHeap) Len() int { return len(h.jobPile) }

// TODO: I shouldn't be calling makespan all the time
// Make it so the first time calculates, then it marks the job as clean/dirty
func (h makespanHeap) Less(i, j int) bool {
	return h.jobPile[i].makespan < h.jobPile[j].makespan
}

func (h makespanHeap) Swap(i, j int) { h.jobPile[i], h.jobPile[j] = h.jobPile[j], h.jobPile[i] }

func (h *makespanHeap) Push(x interface{}) {
	h.jobPile = append(h.jobPile, x.(*makespanJob))
}

func (h *makespanHeap) Pop() interface{} {
	old := h.jobPile
	n := len(old)
	x := old[n-1]
	h.jobPile = old[0 : n-1]
	return x
}

func (h makespanHeap) Top() *job.Job {
	return &h.jobPile[0].Job
}

type MakespanScheduler struct {
	heap     makespanHeap
	topology topology.Topology
	jobs     map[string]*job.Job
	bestDcs  func(file.File, topology.Topology, int) []transferCenter
}

func NewMakespanScheduler(t topology.Topology, bestDcs func(file.File, topology.Topology, int) []transferCenter) *MakespanScheduler {
	scheduler := &MakespanScheduler{
		topology: t,
		jobs:     make(map[string]*job.Job),
		bestDcs:  bestDcs,
	}
	heap.Init(&scheduler.heap)
	return scheduler
}

func (scheduler *MakespanScheduler) Add(j *job.Job) {
	logger.Debugf("%p.Add(%p)", scheduler, j)
	var msJob makespanJob
	msJob.Job = *j
	msJob.bestDcs = scheduler.bestDcs
	sort.Slice(msJob.Job.Tasks, func(i, k int) bool { return msJob.Job.Tasks[i].Duration < msJob.Job.Tasks[k].Duration })
	msJob.tasks = make([]scheduledTask, len(msJob.Tasks))
	for i, t := range msJob.Tasks {
		msJob.tasks[i].duration = t.Duration
	}
	scheduler.heap.Push(&msJob)
	scheduler.jobs[j.Id] = &msJob.Job
}

func (scheduler *MakespanScheduler) Update(now uint64) {
	logger.Debugf("%p.Update(%v)", scheduler, now)
	for _, j := range scheduler.heap.jobPile {
		j.updateMakespan(scheduler.topology, now)
	}
	heap.Init(&scheduler.heap)
}

func (scheduler *MakespanScheduler) Schedule(now uint64) []event.Event {
	logger.Debugf("%p.Schedule(%v)", scheduler, now)
	events := make([]event.Event, 0)
	scheduler.Update(now)

	for scheduler.heap.Len() > 0 {
		top := scheduler.heap.Top()
		dcs := scheduler.bestDcs(top.File, scheduler.topology, int(top.Cpus))
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
					logger.Infof("task ending at %p", node)
					taskEnd.Process()
					hosted = true
					if node.QueueLen() == 1 {
						logger.Infof("adding node %p", node)
						events = append(events, node)
					}
					break
				}
			}
			if !hosted {
				return events
			}

		}
		heap.Pop(&scheduler.heap)
		// try to host all tasks of top job
		// if success, pop it
	}

	return events
}

func (scheduler MakespanScheduler) Results() map[string]*job.Job {
	return scheduler.jobs
}

func NewGeoDis(t topology.Topology) *MakespanScheduler {
	return NewMakespanScheduler(t, fullBestDcs)
}

func presentBestDcs(f file.File, t topology.Topology, cost int) []transferCenter {
	res := make([]transferCenter, 0)

	for i, dc := range t.DataCenters {
		if transferTime(f.Size, t, f.Locations[0], i) == 0 {
			tc := transferCenter{
				transferTime: 0,
				capacity:     dc.JobCapacity(cost),
				freeJobSlots: dc.JobAvailability(cost),
				dataCenter:   dc,
			}
			if tc.capacity > 0 {
				res = append(res, tc)
			}
		}
	}
	if len(res) == 0 {
		logger.Fatalf("Job using file %s cannot be scheduled on any data center", f.Id)
	}
	return res
}

func NewSwag(t topology.Topology) *MakespanScheduler {
	return NewMakespanScheduler(t, presentBestDcs)
}
