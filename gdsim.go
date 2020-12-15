package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"strings"

	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/scheduler"
	"github.com/dsfalves/gdsim/simulator"
	"github.com/dsfalves/gdsim/topology"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func loadFiles(filename string) (map[string]file.File, error) {
	f, err := os.Open(filename)
	check(err)
	defer f.Close()

	return file.Load(f)
}

func loadJobs(filename string, files map[string]file.File) ([]job.Job, error) {
	fileReader, err := os.Open(filename)
	check(err)
	defer fileReader.Close()

	return job.Load(fileReader, files)
}

func loadTopology(filename string) (*topology.Topology, error) {
	reader, err := os.Open(filename)
	check(err)
	defer reader.Close()
	return topology.Load(reader)
}

func printResults(results map[string]*job.Job) {

	for id, j := range results {
		tasks := make([]string, len(j.Scheduled))
		for i, task := range j.Scheduled {
			tasks[i] = fmt.Sprintf("('%s', '%s', %v, %v, %v)", j.File.Id, task.Location, j.Submission, task.Start, task.Start+task.Duration)
		}
		fmt.Printf("%s %v [%v]\n", id, j.Submission, strings.Join(tasks, ", "))
	}
}

func printFiles(files map[string]file.File) {
	fmt.Print("{")
	for key, value := range files {
		locations := make([]string, len(value.Locations))
		for i, loc := range value.Locations {
			locations[i] = fmt.Sprintf("'DC%v'", loc)
		}
		fmt.Printf("'%s': (%v, [%s])", key, value.Size, strings.Join(locations, ", "))
	}
	fmt.Println("}")
}

func main() {
	schedulerPtr := flag.String("scheduler", "SRPT", "type of scheduler to be used")
	topologyPtr := flag.String("topology", "topology.dat", "topology description file")
	filesPtr := flag.String("files", "files.dat", "files description file")
	window := flag.Float64("window", 3, "scheduling window size")
	cpuProfilePtr := flag.String("profiler", "", "write cpu profiling to file")
	logPtr := flag.String("log", "", "file to record log")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("missing files to run")
	}

	if *logPtr == "" {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	} else {
		file, err := os.Create(*logPtr)
		if err != nil {
			log.Fatalf("error opening topology file %v: %v", *logPtr, err)
		}
		log.SetOutput(file)
	}

	topo, err := loadTopology(*topologyPtr)
	check(err)
	files, err := loadFiles(*filesPtr)
	check(err)
	printFiles(files)

	filename := flag.Args()[0]
	jobs, err := loadJobs(filename, files)
	check(err)

	var sched scheduler.Scheduler
	switch *schedulerPtr {
	case "GEODIS":
		sched = scheduler.NewGeoDis(*topo)
	case "SWAG":
		sched = scheduler.NewSwag(*topo)
	case "SRPT":
		sched = scheduler.NewGRPTS(*topo)
	default:
		log.Fatalf("unindentified scheduler %v", *schedulerPtr)
	}

	sim := simulator.New(jobs, files, topo, sched)
	check(err)
	if *cpuProfilePtr != "" {
		f, err := os.Create(*cpuProfilePtr)
		if err != nil {
			log.Fatalf("profiling error: %v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	sim.Run(*window)
	printResults(sched.Results())
}
