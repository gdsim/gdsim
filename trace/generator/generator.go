package main

import (
	"fmt"
	"github.com/dsfalves/simulator/job"
	"gonum.org/v1/gonum/stat/distuv"
	"log"
	"math"
	"math/rand"
)

type Job struct {
	id string
	job.Job
}

type sizeDist interface {
	Rand() float64
}

func Size(s sizeDist) int {
	return int(math.Ceil(s.Rand()))
}

func createNumTasks() int {
	uni := distuv.Uniform{Min: 0, Max: 100}
	num := uni.Rand()
	var x sizeDist
	if num < 6.93 {
		x = &distuv.Uniform{Min: 0, Max: 150}
	} else if num < 23.15+6.93 {
		x = &distuv.Uniform{Min: 150, Max: 600}
	} else {
		x = &distuv.Uniform{Min: 600, Max: 7000}
	}
	return Size(x)
}

func createTaskDuration(source rand.Source) uint64 {
	p := distuv.Pareto{Xm: 1.259, Alpha: 2.7}

	duration := uint64(math.Trunc(p.Rand()))
	return duration
}

func createCpus(source rand.Source) uint {
	x := distuv.Uniform{Min: 1, Max: 32}
	cpus := uint(math.Trunc(x.Rand()))
	return cpus
}

func createInterarrival(source rand.Source) uint64 {
	x := distuv.Poisson{Lambda: 5}
	return uint64(math.Trunc(x.Rand()))
}

func chooseFile(source rand.Source, files []fakeFile) string {
	s := float64(2)
	z := rand.NewZipf(rand.New(source), s, 1, uint64(len(files)-1))
	selected := z.Uint64()
	return files[selected].id
}

func createJob(source rand.Source, files []fakeFile) Job {
	numTasks := createNumTasks()
	j := Job{"", job.Job{Tasks: make([]*job.Task, numTasks)}}

	j.Cpus = createCpus(source)
	j.Submission = createInterarrival(source)
	j.File = chooseFile(source, files)

	for i := range j.Tasks {
		t := &job.Task{Duration: createTaskDuration(source)}
		j.Tasks[i] = t
	}

	return j
}

func (j Job) String() string {
	s := fmt.Sprintf("%v %v %v %v", j.id, j.Cpus, j.Submission, j.File)
	for _, t := range j.Tasks {
		s = fmt.Sprintf("%v %v", s, t.Duration)
	}
	return s
}

func createJobs(source rand.Source, total uint, files []fakeFile) []Job {
	jobs := make([]Job, total)
	for i := range jobs {
		jobs[i] = createJob(source, files)
		jobs[i].id = fmt.Sprintf("job%v", i+1)
	}
	return jobs
}

type fakeFile struct {
	id        string
	size      uint64
	locations []uint
}

func newFile(source rand.Source, nDCs uint) fakeFile {
	var f fakeFile
	f.size = createFileSize(source)
	f.locations = selectLocations(source, nDCs)
	return f
}

func (f fakeFile) String() string {
	s := fmt.Sprintf("%v %v", f.id, f.size)
	for _, l := range f.locations {
		s = fmt.Sprintf("%v %v", s, l)
	}
	return s
}

func createFileSize(source rand.Source) uint64 {
	p := distuv.Pareto{Xm: 1.259, Alpha: 2.7}

	size := uint64(math.Trunc(p.Rand()))
	return size
}

func selectLocations(source rand.Source, nDC uint) []uint {
	var s float64 = 2
	z := rand.NewZipf(rand.New(source), s, 1, uint64(nDC-1))
	selected := z.Uint64() + 1
	locations := make([]uint, nDC)
	for i := range locations {
		locations[i] = uint(i)
	}
	rand.Shuffle(len(locations), func(i, j int) { locations[i], locations[j] = locations[j], locations[i] })
	return locations[:selected]
}

func createFiles(source rand.Source, total, nDCs uint) []fakeFile {
	res := make([]fakeFile, total)

	for i := range res {
		res[i] = newFile(source, nDCs)
		res[i].id = fmt.Sprintf("file%v", i+1)
	}

	return res
}

func printJobs(filename string, data []Job) error {

	for _, obj := range data {
		fmt.Printf("%v\n", obj)
	}

	return nil
}

func printFiles(filename string, data []fakeFile) error {

	for _, obj := range data {
		fmt.Printf("%v\n", obj)
	}

	return nil
}

func main() {
	var seed int64 = 0
	source := rand.NewSource(seed)
	jobName := "job.dat"
	fileName := "file.dat"
	var total uint = 10
	var nDCs uint = 8

	files := createFiles(source, total, nDCs)
	jobs := createJobs(source, total, files)
	if err := printFiles(fileName, files); err != nil {
		log.Fatalf("error creating %v: %v", fileName, err)
	}
	if err := printJobs(jobName, jobs); err != nil {
		log.Fatalf("error creating %v: %v", jobName, err)
	}
}
