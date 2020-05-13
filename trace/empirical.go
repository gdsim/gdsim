package trace

import (
	"encoding/gob"
	"fmt"
	"github.com/dsfalves/simulator/job"
	"gonum.org/v1/gonum/stat/distuv"
	"math"
	"os"
)

type TraceNTG struct {
	NumTasks []uint
}

func NewTraceNTG(jobs []*job.Job) TraceNTG {
	traceNTG := TraceNTG{NumTasks: make([]uint, 0)}

	for _, j := range jobs {
		traceNTG.NumTasks = append(traceNTG.NumTasks, uint(len(j.Tasks)))
	}

	return traceNTG
}

func (ntg TraceNTG) SaveTraceNTG(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error opening %v: %v", filename, err)
	}
	enc := gob.NewEncoder(file)
	if err := enc.Encode(ntg); err != nil {
		return fmt.Errorf("error encoding %v: %v", filename, err)
	}
	return nil
}

func LoadTraceNTG(filename string) (*TraceNTG, error) {
	ntg := &TraceNTG{}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening %v: %v", filename, err)
	}
	dec := gob.NewDecoder(file)
	if err := dec.Decode(ntg); err != nil {
		return nil, fmt.Errorf("error decoding %v: %v", filename, err)
	}
	return ntg, nil
}

func (ntg TraceNTG) CreateNumTasks() uint {
	uni := distuv.Uniform{Min: 0, Max: float64(len(ntg.NumTasks))}
	idx := int(math.Floor(uni.Rand()))
	return ntg.NumTasks[idx]
}
