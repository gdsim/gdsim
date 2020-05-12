package main

import (
	"log"
	"math/rand"
)

func main() {
	var seed int64 = 0
	source := rand.NewSource(seed)
	jobName := "job.dat"
	fileName := "file.dat"
	var total uint = 10
	var nDCs uint = 8

	files := createFiles(source, total, nDCs)
	jobs := createJobs(source, total, files)
	if err := printFiles(fileName, files); err != nil {
		log.Fatalf("error creating %v: %v", fileName, err)
	}
	if err := printJobs(jobName, jobs); err != nil {
		log.Fatalf("error creating %v: %v", jobName, err)
	}
}
