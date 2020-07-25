package job

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Task struct {
	Duration uint64
}

type Job struct {
	Submission uint64
	Cpus       uint
	Tasks      []*Task
	File       string
}

func Load(reader io.Reader) ([]*Job, error) {
	scanner := bufio.NewScanner(reader)
	res := make([]*Job, 0)

	// bandaid fix for larger lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	// TODO: need general fix for long lines
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, " ")
		if len(words) < 5 {
			return nil, fmt.Errorf("failure to read job %d: incomplete line", len(res)+1)
		}
		j := &Job{
			File:  words[3],
			Tasks: make([]*Task, 0),
		}
		cpus, err := strconv.ParseUint(words[1], 0, 0)
		if err != nil {
			return nil, fmt.Errorf("failure to read job %d: %v", len(res)+1, err)
		}
		j.Cpus = uint(cpus)
		j.Submission, err = strconv.ParseUint(words[2], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failure to read job %d: %v", len(res)+1, err)
		}
		for i := 4; i < len(words); i++ {
			d, err := strconv.ParseUint(words[i], 0, 64)
			if err != nil {
				return nil, fmt.Errorf("failure to read job %d: %v", len(res)+1, err)
			}
			t := &Task{Duration: d}
			j.Tasks = append(j.Tasks, t)
		}
		res = append(res, j)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return res, nil
}
