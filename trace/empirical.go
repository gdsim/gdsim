package trace

import (
	"encoding/gob"
	"fmt"
	"github.com/dsfalves/simulator/job"
	"gonum.org/v1/gonum/stat/distuv"
	"math"
	"os"
)

type TraceGenerator struct {
	NTG TraceNTG
}

type TraceNTG struct {
	UintTrace
}

func NewTraceNTG(jobs []*job.Job) TraceNTG {
	traceNTG := TraceNTG{}
	traceNTG.Values = make([]uint, 0)

	for _, j := range jobs {
		traceNTG.Values = append(traceNTG.Values, uint(len(j.Tasks)))
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
	return ntg.Sample()
}

type Uint64Trace struct {
	Values []uint64
}

func (trace Uint64Trace) Sample() uint64 {
	uni := distuv.Uniform{Min: 0, Max: float64(len(trace.Values))}
	idx := int(math.Floor(uni.Rand()))
	return trace.Values[idx]
}

type UintTrace struct {
	Values []uint
}

func (trace UintTrace) Sample() uint {
	uni := distuv.Uniform{Min: 0, Max: float64(len(trace.Values))}
	idx := int(math.Floor(uni.Rand()))
	return trace.Values[idx]
}
