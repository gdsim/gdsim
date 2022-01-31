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
}

func NewAdaptive(t topology.Topology) *AdaptiveScheduler {
	scheduler := &AdaptiveScheduler{}
	scheduler.schedulers = append(scheduler.schedulers, NewSwag(t), NewGeoDis(t))
	scheduler.results = make(map[string]*job.Job)

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

	for _, j := range scheduler.jobs {
		delete(scheduler.results, j.Id)
		for _, sched := range scheduler.schedulers {
			sched.Add(j)
		}
	}
	scheduler.jobs = scheduler.jobs[:0]
	bestIdx := 0
	var bestTime uint64
	for idx, sched := range scheduler.schedulers {
		makespan := sched.Update(now)
		if idx == 0 || bestTime > makespan {
			bestIdx = idx
			bestTime = makespan
		}
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
