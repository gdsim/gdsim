package main

import (
	"log"

	"fmt"
	"github.com/dsfalves/simulator/file"
	"github.com/dsfalves/simulator/job"
	"github.com/dsfalves/simulator/topology"
	"github.com/dsfalves/simulator/trace"
	"os"
)

func main() {
	jobFile := "out.job"

	jobReader, err := os.Open(jobFile)
	if err != nil {
		log.Fatalf("problem opening %v: %v", jobFile, err)
	}
	jobs, err := job.Load(jobReader)
	if err != nil {
		log.Fatalf("problem loading from %v: %v", jobFile, err)
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
	fileTraceFile := "fileTrace.gen"
	traceGen.FSel.SaveTraceFileSelector(fileTraceFile)

	topologyFile := "topology.dat"
	topoReader, err := os.Open(topologyFile)
	if err != nil {
		log.Fatalf("problem opening %v: %v", topologyFile, err)
	}
	topo, err := topology.Load(topoReader)
	if err != nil {
		log.Fatalf("problem loading %v: %v", topologyFile, err)
	}
	fmt.Println(topo)

	fileFile := "out.files"
	fileReader, err := os.Open(fileFile)
	if err != nil {
		log.Fatalf("problem opening %v: %v", fileFile, err)
	}
	files, err := file.Load(fileReader, topo.DataCenters)
	if err != nil {
		log.Fatalf("problem loading from %v: %v", fileFile, err)
	}
	filesList := make([]*file.File, 0, len(files))
	for _, f := range files {
		filesList = append(filesList, f)
	}

	fileTraceGen := trace.FileTraceGenerator{
		SizeGen:     trace.NewTraceSG(filesList),
		LocationSel: trace.NewTraceLS(filesList),
	}
	sizeFile := "sizeTrace.filegen"
	fileTraceGen.SizeGen.SaveTraceSG(sizeFile)
	locationFile := "locationTrace.filegen"
	fileTraceGen.LocationSel.SaveTraceLS(locationFile)
}
