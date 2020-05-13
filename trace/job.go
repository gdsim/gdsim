package trace

import (
	"fmt"
	"github.com/dsfalves/simulator/job"
	"gonum.org/v1/gonum/stat/distuv"
	"math"
	"math/rand"
	"os"
)

type Job struct {
	id string
	job.Job
}

type JobCreator struct {
	NTG NumTasksGenerator
	TDG TaskDurationGenerator
}

type sizeDist interface {
	Rand() float64
}

func Size(s sizeDist) uint {
	return uint(math.Ceil(s.Rand()))
}

type NumTasksGenerator interface {
	CreateNumTasks() uint
}

type SimpleNumTasksGenerator struct {
	Small, Medium float64
}

func (gen SimpleNumTasksGenerator) CreateNumTasks() uint {
	uni := distuv.Uniform{Min: 0, Max: 100}
	num := uni.Rand()
	var x sizeDist
	if num < gen.Small {
		x = &distuv.Uniform{Min: 0, Max: 150}
	} else if num < gen.Small+gen.Medium {
		x = &distuv.Uniform{Min: 150, Max: 600}
	} else {
		x = &distuv.Uniform{Min: 600, Max: 7000}
	}
	return Size(x)
}

type TaskDurationGenerator interface {
	Duration() uint64
}

type ParetoTDG struct {
	Pareto distuv.Pareto
}

func (p ParetoTDG) Duration() uint64 {
	return uint64(math.Trunc(p.Pareto.Rand()))
}

func StandardPareto() ParetoTDG {
	return ParetoTDG{distuv.Pareto{Xm: 1.259, Alpha: 2.7}}
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

func createJob(source rand.Source, files []fakeFile, jc JobCreator) Job {
	//ntg := SimpleNumTasksGenerator{Small: 6.93, Medium: 23.15}
	numTasks := jc.NTG.CreateNumTasks()
	j := Job{"", job.Job{Tasks: make([]*job.Task, numTasks)}}

	j.Cpus = createCpus(source)
	j.Submission = createInterarrival(source)
	j.File = chooseFile(source, files)

	for i := range j.Tasks {
		t := &job.Task{Duration: jc.TDG.Duration()}
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

func CreateJobs(source rand.Source, total uint, files []fakeFile, jc JobCreator) []Job {
	jobs := make([]Job, total)
	for i := range jobs {
		jobs[i] = createJob(source, files, jc)
		jobs[i].id = fmt.Sprintf("job%v", i+1)
	}
	return jobs
}

func PrintJobs(filename string, data []Job) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	for _, obj := range data {
		fmt.Fprintf(f, "%v\n", obj)
	}

	return nil
}
