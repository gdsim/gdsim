package main

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"

	"flag"
	"os"

	"github.com/dsfalves/gdsim/file"
	"github.com/dsfalves/gdsim/job"

	"github.com/dsfalves/gdsim/trace"
)

func loadFiles(fileFile string) ([]trace.File, error) {
	res := make([]trace.File, 0)
	fileReader, err := os.Open(fileFile)
	if err != nil {
		return nil, fmt.Errorf("problem opening %v: %v", fileFile, err)
	}
	scanner := bufio.NewScanner(fileReader)
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, " ")
		s, err := strconv.ParseUint(words[1], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
		}
		f := file.New(words[0], s)
		l := make([]uint, 0)
		for i := 2; i < len(words); i++ {
			k, err := strconv.ParseUint(words[i], 0, 0)
			if err != nil {
				return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
			}
			l = append(l, uint(k))
		}
		res = append(res, trace.File{
			Locations: l,
			File:      f,
		})
	}

	return res, nil
}

func main() {
	jobFile := flag.String("jobs", "out.jobs", "file with job data to be extracted")
	fileFile := flag.String("files", "out.files", "file with file data to be extracted")
	flag.Parse()

	filesList, err := loadFiles(*fileFile)
	if err != nil {
		log.Fatalf("error loading file data from %v: %v", *fileFile, err)
	}

	// TODO: add code to generate tracefilelist
	fileTraceGen := trace.FileTraceGenerator{
		SizeGen:     trace.NewTraceSG(filesList),
		LocationSel: trace.NewTraceLS(filesList),
	}
	sizeFile := "sizeTrace.filegen"
	fileTraceGen.SizeGen.SaveTraceSG(sizeFile)
	locationFile := "locationTrace.filegen"
	fileTraceGen.LocationSel.SaveTraceLS(locationFile)

	jobReader, err := os.Open(*jobFile)
	if err != nil {
		log.Fatalf("problem opening %v: %v", *jobFile, err)
	}
	files := make(map[string]file.File)
	for _, f := range filesList {
		files[f.File.Id()] = f.File
	}
	jobs, err := job.Load(jobReader, files)
	if err != nil {
		log.Fatalf("problem loading from %v: %v", *jobFile, err)
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

}
