package main

import (
	"log"

	"github.com/dsfalves/simulator/job"
	"github.com/dsfalves/simulator/trace"
	"os"
)

func main() {
	jobFile := "job.dat"

	jobReader, err := os.Open(jobFile)
	if err != nil {
		log.Fatal("problem opening %v: %v", jobFile, err)
	}
	jobs, err := job.Load(jobReader)
	if err != nil {
		log.Fatal("problem loading from %v: %v", jobFile, err)
	}

	ntFile := "numTrace.gen"
	ntg := trace.NewTraceNTG(jobs)
	ntg.SaveTraceNTG(ntFile)
}
