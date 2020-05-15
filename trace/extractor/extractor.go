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

	traceGen := trace.TraceGenerator{
		NTG:  trace.NewTraceNTG(jobs),
		TDG:  trace.NewTraceTDG(jobs),
		CGen: trace.NewTraceCPUGen(jobs),
		DGen: trace.NewTraceDelayGen(jobs),
		FSel: trace.NewTraceFileSelector(jobs),
	}

	ntFile := "numTrace.gen"
	traceGen.NTG.SaveTraceNTG(ntFile)
	tdFile := "durationTrace.gen"
	traceGen.TDG.SaveTraceTDG(tdFile)
	cpuFile := "cpuTrace.gen"
	traceGen.CGen.SaveTraceCPUGen(cpuFile)
	delayFile := "delayTrace.gen"
	traceGen.DGen.SaveTraceDelayGen(delayFile)
	fileFile := "fileTrace.gen"
	traceGen.FSel.SaveTraceFileSelector(fileFile)
}
