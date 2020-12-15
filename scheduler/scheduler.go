package scheduler

import (
	"fmt"
	"sort"

	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/log"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
)

var logger log.Context

func init() {
	logger = log.New("scheduler")
}

type jobHeap []*job.Job

// TODO: I shouldn't be calling rpt all the time
// Make it so the first time calculates, then it marks the job as clean/dirty
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

func (h jobHeap) Top() *job.Job {
	return h[0]
}

func transferTime(size uint64, t topology.Topology, from, to int) uint64 {
	if from == to {
		return 0
	}
	return size / t.Speeds[from][to]
}

type transferCenter struct {
	transferTime           uint64
	freeJobSlots, capacity int
	dataCenter             *topology.DataCenter
}

/*
Returns a list of data centers suitable for running a job that requires file f,
sorted by transfer time in topology t and with capacity according to cost.
*/
func fullBestDcs(f file.File, t topology.Topology, cost int) []transferCenter {
	res := make([]transferCenter, len(t.DataCenters))

	for i := range t.DataCenters {
		res[i].dataCenter = t.DataCenters[i]
		res[i].transferTime = transferTime(f.Size, t, f.Locations[0], i)
		res[i].capacity = t.DataCenters[i].JobCapacity(cost)
		res[i].freeJobSlots = t.DataCenters[i].JobAvailability(cost)
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
	where           int
	job             *job.Job
}

func (event taskEndEvent) End() uint64 {
	return event.start + event.duration
}

func (event taskEndEvent) Cpus() int {
	return event.cpus
}

func (event taskEndEvent) Process() []event.Event {
	logger.Debugf("%v.Process()", event)
	event.job.Scheduled = append(event.job.Scheduled, job.DoneTask{
		Start:    event.start,
		Duration: event.duration,
		Location: fmt.Sprintf("DC%v", event.where),
	})
	logger.Infof("added event to Scheduled - len(Scheduled) = %v\n", len(event.job.Scheduled))
	return nil
}

type Scheduler interface {
	//Pop() *job.Task
	Add(t *job.Job)
	Schedule(now uint64) []event.Event
	Results() map[string]*job.Job
}
