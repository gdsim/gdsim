package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"

	"github.com/dsfalves/simulator/job"
	"gonum.org/v1/gonum/stat/distuv"
)

type Event int

const (
	Submit Event = iota
	Schedule
	Evict
	Fail
	Finish
	Kill
	Lost
	UpdatePending
	UpdateRunning
)

func (ev Event) String() string {
	table := map[Event]string{
		Submit:        "SUBMIT",
		Schedule:      "SCHEDULE",
		Evict:         "EVICT",
		Fail:          "FAIL",
		Finish:        "FINISH",
		Kill:          "KILL",
		Lost:          "LOST",
		UpdatePending: "UPDATE_PENDING",
		UpdateRunning: "UPDATE_RUNNING",
	}
	label, ok := table[ev]
	if !ok {
		return "Unknown"
	}
	return label
}

func check(err error) {
	if err != nil {
		fmt.Errorf("error found: %x\n", err)
		os.Exit(1)
	}
}

type Record struct {
	ts, jobId, taskId int64
	typeId            Event
	cpus, size        int
	user              string
}

func getRecord(line []string) Record {

	ts, err := strconv.ParseInt(line[0], 10, 64)
	check(err)
	ts /= int64(math.Pow(10, 6))
	jobId, err := strconv.ParseInt(line[2], 10, 64)
	check(err)
	taskId, err := strconv.ParseInt(line[3], 10, 64)
	check(err)
	typeId64, err := strconv.ParseInt(line[5], 10, 0)
	check(err)
	typeId := Event(typeId64)
	norm_cpus, err := strconv.ParseFloat(line[9], 0)
	check(err)
	sizef64, err := strconv.ParseFloat(line[11], 0)
	check(err)
	size := int(math.Trunc(sizef64 * math.Pow(10, 12)))
	cpus := int(math.Trunc(norm_cpus * 32))
	if cpus == 0 {
		cpus = 1
	}
	if cpus < 0 {
		fmt.Errorf("bad number of cpus %d\n", cpus)
		os.Exit(1)
	}
	return Record{
		ts:     ts,
		jobId:  jobId,
		taskId: taskId,
		typeId: typeId,
		cpus:   cpus,
		user:   line[6],
		size:   size,
	}
}

func (record *Record) Task() *job.Task {
	task := &job.Task{
		Start: record.ts,
	}
	return task
}

func (record *Record) Job() *job.Job {
	job := &job.Job{
		Submission: record.ts,
		Tasks:      make(map[int64]*job.Task),
		Cpus:       record.cpus,
		Size:       record.size,
		User:       record.user,
	}

	job.tasks[record.taskId] = record.Task()

	return job
}

func (record *Record) updateTask(task *job.Task) {
	switch record.typeId {
	case Schedule:
		task.start = record.ts
	case Finish, Kill, Fail, Evict:
		task.duration = record.ts - task.start
	}
}

func (record *Record) updateJobList(jobList map[int64]*job.Job) {
	job, ok := jobList[record.jobId]
	if !ok {
		job = record.Job()
		jobList[record.jobId] = job
	}
	if record.typeId == Submit && job.submission > record.ts {
		job.submission = record.ts
	}
	task, ok := job.tasks[record.taskId]
	if !ok {
		job.tasks[record.taskId] = record.Task()
	} else {
		record.updateTask(task)
	}
}

func main() {
	p := distuv.Pareto{Xm: 1.259, Alpha: 2.7}
	jobList := make(map[int64]*job.Job)
	for i := 1; i <= 200; i++ {
		filename := fmt.Sprintf("part-%05d-of-00500.csv.gz", i)
		file, err := os.Open(filename)
		check(err)
		defer file.Close()

		gz, err := gzip.NewReader(file)
		check(err)
		defer gz.Close()

		reader := csv.NewReader(gz)
		line, err := reader.Read()
		check(err)
		for len(line) > 0 && err == nil {
			record := getRecord(line)

			line, err = reader.Read()
			record.updateJobList(jobList)
		}
	}
	ids := make([]int64, 0, len(jobList))
	for id, _ := range jobList {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return jobList[ids[a]].submission < jobList[ids[b]].submission })
	for _, id := range ids {
		job := jobList[id]
		fmt.Printf("%d %d %d %s %d", id, job.cpus, job.submission, job.user, job.size)
		for _, task := range job.tasks {
			if task.duration == 0 {
				task.duration = int64(math.Trunc(p.Rand()))
			}
			fmt.Printf(" %d", task.duration)
		}
		fmt.Println()
	}
}
