package main

import (
	"flag"
	"log"
	"os"

	"fmt"
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
		for _, task := range j.Scheduled {
			tasks = append(tasks, fmt.Sprintf("(%s, %s, %v, %v, %v)", j.File.Id, task.Location, j.Submission, task.Start, task.Start+task.Duration))
		}
		fmt.Printf("%s %v %v\n", id, j.Submission, tasks)
	}
}

func main() {
	schedulerPtr := flag.String("scheduler", "SRPT", "type of scheduler to be used")
	topologyPtr := flag.String("topology", "topology.dat", "topology description file")
	filesPtr := flag.String("files", "files.dat", "files description file")
	window := flag.Float64("window", 3, "scheduling window size")
	flag.Parse()
	fmt.Println(*topologyPtr)
	fmt.Println(*filesPtr)
	fmt.Println(flag.Args())
	if len(flag.Args()) < 1 {
		log.Fatal("missing files to run")
	}

	topo, err := loadTopology(*topologyPtr)
	check(err)
	files, err := loadFiles(*filesPtr)
	check(err)

	filename := flag.Args()[0]
	fmt.Println(filename, *schedulerPtr)
	jobs, err := loadJobs(filename, files)
	check(err)
	scheduler := scheduler.NewGRPTS(*topo)
	sim := simulator.New(jobs, files, topo, &scheduler)
	check(err)
	//schedule, err := run(jobs, files, topo)
	//print(schedule)
	sim.Run(*window)
	printResults(scheduler.Results())
}
