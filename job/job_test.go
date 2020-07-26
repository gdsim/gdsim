package job

import (
	"strings"
	"testing"
)

func validJob(t *testing.T, job Job, id string, cpus uint, submission uint64, tasks []uint64, file string) bool {
	if job.Id != id {
		t.Errorf("expected job.Id = %v, found %v", job.Id, id)
	}
	if job.Submission != submission {
		t.Errorf("expected job.Submission = %v, found %v", job.Submission, submission)
	}
	if job.Cpus != cpus {
		t.Errorf("expected job.Cpus = %v, found %v", job.Cpus, cpus)
	}
	if len(job.Tasks) != len(tasks) {
		t.Errorf("expected len(job.Tasks) = %v, found %v", len(tasks), len(job.Tasks))
	}
	for i, task := range job.Tasks {
		if task.Duration != tasks[i] {
			t.Errorf("expected job.Tasks[i].Duration = %v, found %v", tasks[i], task.Duration)
		}
	}
	if job.File != file {
		t.Errorf("expected job.File = %v, found %v", job.File, file)
	}
	return true
}

func TestLoad(t *testing.T) {
	sample := "j1 1 0 f1 1 2\nj2 2 1 f2 7"
	reader := strings.NewReader(sample)

	jobs, err := Load(reader)
	if err != nil {
		t.Errorf("expected no error for sample '%v', found '%v'", sample, err)
	}
	numJobs := len(jobs)
	if numJobs != 2 {
		t.Errorf("expected len(jobs) = %v, found %v", 2, numJobs)
	}
	validJob(t, jobs[0], "j1", 1, 0, []uint64{1, 2}, "f1")
	validJob(t, jobs[1], "j2", 2, 1, []uint64{7}, "f2")
}
