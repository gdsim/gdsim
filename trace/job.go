package trace

import (
	"fmt"
	"math"
	"math/rand"
	"os"

	"github.com/dsfalves/gdsim/job"
	"gonum.org/v1/gonum/stat/distuv"
)

type Job struct {
	id string
	job.Job
}

type JobCreator struct {
	NTG  NumTasksGenerator
	TDG  TaskDurationGenerator
	CGen CPUGenerator
	DGen DelayGenerator
	FSel FileSelector
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

type CPUGenerator interface {
	CPUs() uint
}

type SimpleCPUGen struct {
	Uniform distuv.Uniform
}

func CreateSimpleCG() SimpleCPUGen {
	return SimpleCPUGen{distuv.Uniform{Min: 1, Max: 32}}
}

func (gen SimpleCPUGen) CPUs() uint {
	return uint(math.Trunc(gen.Uniform.Rand()))
}

type DelayGenerator interface {
	Delay() uint64
}

type PoissonDelayGenerator struct {
	Poisson distuv.Poisson
}

func CreatePoissonDG() PoissonDelayGenerator {
	return PoissonDelayGenerator{distuv.Poisson{Lambda: 5}}
}

func (gen PoissonDelayGenerator) Delay() uint64 {
	return uint64(math.Trunc(gen.Poisson.Rand()))
}

type FileSelector interface {
	File([]File) File
}

type ZipfFileSelector struct {
	Zipf *rand.Zipf
}

func CreateZipfFS(source rand.Source, max uint64) ZipfFileSelector {
	zipf := rand.NewZipf(rand.New(source), float64(2), 1, max)
	return ZipfFileSelector{zipf}
}

func (gen ZipfFileSelector) File(files []File) File {
	selected := gen.Zipf.Uint64()
	for int(selected) >= len(files) {
		selected = gen.Zipf.Uint64()
	}
	return files[selected]
}

func (jc JobCreator) createJob(files []File) Job {
	//ntg := SimpleNumTasksGenerator{Small: 6.93, Medium: 23.15}
	numTasks := jc.NTG.CreateNumTasks()
	j := Job{"", job.Job{Tasks: make([]job.Task, numTasks)}}

	j.Cpus = jc.CGen.CPUs()
	j.Submission = jc.DGen.Delay()
	j.File = jc.FSel.File(files).File

	for i := range j.Tasks {
		t := job.Task{Duration: jc.TDG.Duration()}
		j.Tasks[i] = t
	}

	return j
}

func (j Job) String() string {
	s := fmt.Sprintf("%v %v %v %v %v", j.id, j.Cpus, j.Submission, j.File.Id(), j.File.Size())
	for _, t := range j.Tasks {
		s = fmt.Sprintf("%v %v", s, t.Duration)
	}
	return s
}

func (jc JobCreator) CreateJobs(total uint, files []File) []Job {
	jobs := make([]Job, total)
	for i := range jobs {
		jobs[i] = jc.createJob(files)
		jobs[i].id = fmt.Sprintf("job%v", i+1)
	}
	return jobs
}

func SaveJobs(filename string, data []Job) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, obj := range data {
		fmt.Fprintf(f, "%v\n", obj)
	}

	return nil
}
