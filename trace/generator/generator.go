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

	jcFile := "numTrace.gen"
	ntg, err := trace.LoadTraceNTG(jcFile)
	if err != nil {
		log.Fatal(err)
	}
	files := trace.CreateFiles(source, total, nDCs)

	jobCreator := trace.JobCreator{
		NTG:  ntg,
		TDG:  trace.StandardPareto(),
		CGen: trace.CreateSimpleCG(),
		DGen: trace.CreatePoissonDG(),
		FSel: trace.CreateZipfFS(source, files),
	}

	jobs := jobCreator.CreateJobs(total)
	if err := trace.SaveFiles(fileName, files); err != nil {
		log.Fatalf("error creating %v: %v", fileName, err)
	}
	if err := trace.SaveJobs(jobName, jobs); err != nil {
		log.Fatalf("error creating %v: %v", jobName, err)
	}
}
