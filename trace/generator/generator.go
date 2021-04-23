package main

import (
	"log"
	"math/rand"

	"flag"

	"github.com/dsfalves/gdsim/trace"
)

func main() {
	skew := flag.Float64("skew", 2, "skew for file distribution")
	total := flag.Uint("total", 1000, "number of jobs to be generated")
	jobName := flag.String("jobs", "job.dat", "name of the file with generated jobs")
	fileName := flag.String("files", "file.dat", "name of the file with generated files")
	seed := flag.Int64("seed", 0, "random seed to be used")
	flag.Parse()

	source := rand.NewSource(*seed)
	var nDCs uint = 8

	jcFile := "numTrace.gen"
	ntg, err := trace.LoadTraceNTG(jcFile)
	if err != nil {
		log.Fatal(err)
	}
	tdFile := "durationTrace.gen"
	tdg, err := trace.LoadTraceTDG(tdFile)
	if err != nil {
		log.Fatal(err)
	}
	cpuFile := "cpuTrace.gen"
	cgen, err := trace.LoadTraceCPUGen(cpuFile)
	if err != nil {
		log.Fatal(err)
	}
	delayFile := "delayTrace.gen"
	dgen, err := trace.LoadTraceDelayGen(delayFile)
	if err != nil {
		log.Fatal(err)
	}
	fileFile := "fileTrace.gen"
	fsel, err := trace.LoadTraceFileSelector(fileFile)
	if err != nil {
		log.Fatal(err)
	}
	sizeFile := "sizeTrace.filegen"
	sg, err := trace.LoadTraceSG(sizeFile)
	if err != nil {
		log.Fatal(err)
	}
	/*
		locationFile := "locationTrace.filegen"
		ls, err := trace.LoadTraceLS(locationFile)
		if err != nil {
			log.Fatal(err)
		}
	*/

	fileCreator := trace.FileCreator{
		SizeGen:     sg, //trace.CreateParetoSizeGenerator(),
		LocationSel: trace.CreateZipfSLS(source, nDCs, *skew),
		//LocationSel: ls, //trace.CreateZipfLS(source, nDCs),
	}

	files := fileCreator.CreateFiles(source, fsel.Size(), nDCs)

	jobCreator := trace.JobCreator{
		NTG:  ntg,
		TDG:  tdg,  //trace.StandardPareto(),
		CGen: cgen, //trace.CreateSimpleCG(),
		DGen: dgen, //trace.CreatePoissonDG(),
		FSel: fsel, //trace.CreateZipfFS(source, files),
	}

	jobs := jobCreator.CreateJobs(*total, files)
	if err := trace.SaveFiles(*fileName, files); err != nil {
		log.Fatalf("error creating %v: %v", fileName, err)
	}
	if err := trace.SaveJobs(*jobName, jobs); err != nil {
		log.Fatalf("error creating %v: %v", jobName, err)
	}
}
