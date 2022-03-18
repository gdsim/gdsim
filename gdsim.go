package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime/pprof"
	"strings"

	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"
	"github.com/dsfalves/gdsim/log"
	"github.com/dsfalves/gdsim/network"
	"github.com/dsfalves/gdsim/scheduler"
	"github.com/dsfalves/gdsim/simulator"
	"github.com/dsfalves/gdsim/topology"
)

var logger log.Context

func check(err error) {
	if err != nil {
		logger.Fatalf("%v", err)
	}
}

func loadFiles(filename string, topo *topology.Topology, nw network.Network) (map[string]file.File, error) {
	f, err := os.Open(filename)
	check(err)
	defer f.Close()

	return file.Load(f, topo, nw)
}

func loadJobs(filename string, files map[string]file.File) ([]job.Job, error) {
	fileReader, err := os.Open(filename)
	check(err)
	defer fileReader.Close()

	return job.Load(fileReader, files)
}

func loadTopology(filename string, nw network.Network) (*topology.Topology, error) {
	reader, err := os.Open(filename)
	check(err)
	defer reader.Close()
	return topology.LoadFifo(reader, nw)
}

func printResults(results map[string]*job.Job) {

	for id, j := range results {
		tasks := make([]string, len(j.Scheduled))
		for i, task := range j.Scheduled {
			tasks[i] = fmt.Sprintf("('%s', '%s', %v, %v, %v)", j.File.Id(), task.Location, j.Submission, task.Start, task.Start+task.Duration)
		}
		fmt.Printf("%s %v [%v]\n", id, j.Submission, strings.Join(tasks, ", "))
	}
}

func printFiles(files map[string]file.File, topo *topology.Topology) {
	fmt.Print("{")
	for key, value := range files {
		locations := make([]string, 0, len(topo.DataCenters))
		for i, dc := range topo.DataCenters {
			if dc.Container().Has(key) {
				locations = append(locations, fmt.Sprintf("'DC%v'", i))
			}
		}
		fmt.Printf("'%s': (%v, [%s])", key, value.Size(), strings.Join(locations, ", "))
	}
	fmt.Println("}")
}

func main() {
	logger = log.New("main")
	schedulerPtr := flag.String("scheduler", "SRPT", "type of scheduler to be used")
	topologyPtr := flag.String("topology", "default.topo", "topology description file")
	filesPtr := flag.String("files", "trace.files", "files description file")
	window := flag.Uint64("window", 3, "scheduling window size")
	cpuProfilePtr := flag.String("profiler", "", "write cpu profiling to file")
	logPtr := flag.String("log", "", "file to record log")
	ratioPtr := flag.Float64("ratio", 0.25, "ratio for adaptive scheduler -- must be larger than 0, and will be ignored if not using the adaptive scheduler")
	flag.Parse()
	if len(flag.Args()) < 1 {
		logger.Fatalf("missing files to run")
	}
	if *ratioPtr <= 0 {
		logger.Fatalf("invalid ratio value")
	}

	if *logPtr == "" {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)

	} else {
		var file *os.File
		if *logPtr == "-" {
			file = os.Stdout
		} else {
			var err error
			file, err = os.Create(*logPtr)
			if err != nil {
				logger.Fatalf("error opening log file %v: %v", *logPtr, err)
			}
		}
		log.SetLevel(log.DEBUG)
		log.EnableContext("simulator")
		log.EnableContext("topology")
		log.EnableContext("scheduler")
		log.SetOutput(file)
	}

	nw := network.NewSimpleNetwork()
	topo, err := loadTopology(*topologyPtr, &nw)
	check(err)
	files, err := loadFiles(*filesPtr, topo, &nw)
	check(err)
	printFiles(files, topo)

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
	case "ADAPTIVE":
		sched = scheduler.NewAdaptive(*topo, *ratioPtr)
	case "NADAPTIVE":
		sched = scheduler.NewAdaptive2(*topo, *ratioPtr)
	case "RATIO":
		sched = scheduler.NewAdaptive3(*topo, *ratioPtr)
	case "RATIO2":
		sched = scheduler.NewRatio2(*topo, *ratioPtr)
	case "RATIO3":
		sched = scheduler.NewRatio3(*topo, *ratioPtr)
	default:
		logger.Fatalf("unindentified scheduler %v", *schedulerPtr)
	}

	sim := simulator.New(jobs, files, topo, sched, *window)
	check(err)
	if *cpuProfilePtr != "" {
		f, err := os.Create(*cpuProfilePtr)
		if err != nil {
			logger.Fatalf("profiling error: %v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	sim.Run()
	printResults(sched.Results())
}
