package main

import (
	"log"
	"math/rand"

	"github.com/dsfalves/simulator/trace"
)

func main() {
	var seed int64 = 0
	source := rand.NewSource(seed)
	jobName := "job.dat"
	fileName := "file.dat"
	var total uint = 10
	var nDCs uint = 8

	files := trace.CreateFiles(source, total, nDCs)
	jobs := trace.CreateJobs(source, total, files)
	if err := trace.PrintFiles(fileName, files); err != nil {
		log.Fatalf("error creating %v: %v", fileName, err)
	}
	if err := trace.PrintJobs(jobName, jobs); err != nil {
		log.Fatalf("error creating %v: %v", jobName, err)
	}
}
