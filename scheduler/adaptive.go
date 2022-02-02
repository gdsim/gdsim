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

type AdaptiveScheduler struct {
	jobs       []*job.Job
	schedulers []*MakespanScheduler
	results    map[string]*job.Job
	ratio      float64
}

func NewAdaptive(t topology.Topology, ratio float64) *AdaptiveScheduler {
	scheduler := &AdaptiveScheduler{}
	scheduler.schedulers = append(scheduler.schedulers, NewSwag(t), NewGeoDis(t))
	scheduler.results = make(map[string]*job.Job)
	scheduler.ratio = ratio

	return scheduler
}

func (scheduler *AdaptiveScheduler) Add(j *job.Job) {
	logger.Debugf("%p.Add(%p)", scheduler, j)
	scheduler.jobs = append(scheduler.jobs, j)
}

func (scheduler *AdaptiveScheduler) Results() map[string]*job.Job {
	return scheduler.results
}

func (scheduler AdaptiveScheduler) Pending() int {
	return len(scheduler.jobs)
}

func (scheduler *AdaptiveScheduler) Schedule(now uint64) []event.Event {
	logger.Debugf("%p.Schedule(%v)", scheduler, now)

	mean_tasks := 0.0
	for _, j := range scheduler.jobs {
		mean_tasks += float64(len(j.Tasks))
	}
	mean_tasks /= float64(len(scheduler.jobs))

	var_tasks := 0.0
	for _, j := range scheduler.jobs {
		var_tasks += float64(len(j.Tasks)) - mean_tasks
	}
	var_tasks /= float64(len(scheduler.jobs) - 1)

	var bestIdx int
	if var_tasks > mean_tasks*scheduler.ratio {
		bestIdx = 0
	} else {
		bestIdx = 1
	}
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
