package scheduler

/*
   This is a scheduler that will select from existing scheduler to better adapt to the properties of incoming jobs.
   This implementation will attempt to do so by estimating the makespan from each scheduler and choosing the one with the smallest makespan.
*/

import (
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
)

type Ratio3Scheduler struct {
	topology   topology.Topology
	jobs       []*job.Job
	schedulers []*MakespanScheduler
	results    map[string]*job.Job
	ratio      float64
}

func NewRatio3(t topology.Topology, ratio float64) *Ratio3Scheduler {
	scheduler := &Ratio3Scheduler{}
	scheduler.topology = t
	scheduler.schedulers = append(scheduler.schedulers, NewSwag(t), NewGeoDis(t))
	scheduler.results = make(map[string]*job.Job)
	scheduler.ratio = ratio

	return scheduler
}

func (scheduler *Ratio3Scheduler) Add(j *job.Job) {
	logger.Debugf("%p.Add(%p)", scheduler, j)
	scheduler.jobs = append(scheduler.jobs, j)
}

func (scheduler *Ratio3Scheduler) Results() map[string]*job.Job {
	return scheduler.results
}

func (scheduler Ratio3Scheduler) Pending() int {
	return len(scheduler.jobs)
}

func (scheduler *Ratio3Scheduler) Schedule(now uint64) []event.Event {
	logger.Debugf("%p.Schedule(%v)", scheduler, now)

	available := 0
	var required uint = 0

	for _, dc := range scheduler.topology.DataCenters {
		available += dc.JobAvailability(1)
	}

	for _, job := range scheduler.jobs {
		required += job.Cpus
	}

	var bestIdx int
	if float64(required)/float64(available) < scheduler.ratio {
		bestIdx = 0
	} else {
		bestIdx = 1
	}
	for _, j := range scheduler.jobs {
		scheduler.schedulers[bestIdx].Add(j)
	}
	scheduler.jobs = scheduler.jobs[:0]
	events := scheduler.schedulers[bestIdx].Schedule(now)

	for idx, sched := range scheduler.schedulers {
		jobs := sched.heap.Flush()
		if idx == bestIdx {
			for _, job := range jobs {
				scheduler.jobs = append(scheduler.jobs, &job.Job)
			}
		}
	}
	for id, j := range scheduler.schedulers[bestIdx].Results() {
		scheduler.results[id] = j
	}

	return events
}
