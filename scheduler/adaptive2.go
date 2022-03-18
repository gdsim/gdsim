package scheduler

/*
   This is a scheduler that will select from existing scheduler to better adapt to the properties of incoming jobs.
   This implementation will attempt to do so by estimating the makespan from each scheduler and choosing the one with the smallest makespan.
*/

import (
	"math"

	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
)

type Adaptive2Scheduler struct {
	jobs       []*job.Job
	schedulers []*MakespanScheduler
	results    map[string]*job.Job
	ratio      float64
}

func NewAdaptive2(t topology.Topology, ratio float64) *Adaptive2Scheduler {
	scheduler := &Adaptive2Scheduler{}
	scheduler.schedulers = append(scheduler.schedulers, NewSwag(t), NewGeoDis(t))
	scheduler.results = make(map[string]*job.Job)
	scheduler.ratio = ratio

	return scheduler
}

func (scheduler *Adaptive2Scheduler) Add(j *job.Job) {
	logger.Debugf("%p.Add(%p)", scheduler, j)
	scheduler.jobs = append(scheduler.jobs, j)
}

func (scheduler *Adaptive2Scheduler) Results() map[string]*job.Job {
	return scheduler.results
}

func (scheduler Adaptive2Scheduler) Pending() int {
	return len(scheduler.jobs)
}

func (scheduler *Adaptive2Scheduler) Schedule(now uint64) []event.Event {
	logger.Debugf("%p.Schedule(%v)", scheduler, now)

	mean_tasks := 0.0
	delta := 0.0
	delta2 := 0.0
	mean2 := 0.0
	count := 0.0
	for _, j := range scheduler.jobs {
		for _, t := range j.Tasks {
			count += 1
			task_duration := float64(t.Duration)
			delta = task_duration - mean_tasks
			mean_tasks += delta / count
			delta2 = task_duration - mean_tasks
			mean2 += delta * delta2
		}
	}
	var var_tasks float64
	if len(scheduler.jobs) < 2 {
		var_tasks = math.NaN()
	} else {
		var_tasks = mean2 / float64(count-1)
	}

	var bestIdx int
	if var_tasks < mean_tasks*scheduler.ratio {
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
