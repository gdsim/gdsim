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
	NTG  TraceNTG
	TDG  TraceTDG
	CGen TraceCPUGen
	DGen TraceDelayGen
	FSel TraceFileSelector
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

type StringTrace struct {
	Values []string
}

func (trace Uint64Trace) Sample() uint64 {
	uni := distuv.Uniform{Min: 0, Max: float64(len(trace.Values))}
	idx := int(math.Floor(uni.Rand()))
	return trace.Values[idx]
}

func (trace StringTrace) Sample() string {
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

type TraceTDG struct {
	Uint64Trace
}

func NewTraceTDG(jobs []*job.Job) TraceTDG {
	traceTDG := TraceTDG{}
	traceTDG.Values = make([]uint64, 0)

	for _, j := range jobs {
		for _, t := range j.Tasks {
			traceTDG.Values = append(traceTDG.Values, uint64(t.Duration))
		}

	}

	return traceTDG
}

func (ntg TraceTDG) SaveTraceTDG(filename string) error {
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

func LoadTraceTDG(filename string) (*TraceTDG, error) {
	ntg := &TraceTDG{}
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

func (ntg TraceTDG) Duration() uint64 {
	return ntg.Sample()
}

type TraceCPUGen struct {
	UintTrace
}

func NewTraceCPUGen(jobs []*job.Job) TraceCPUGen {
	traceCPUGen := TraceCPUGen{}
	traceCPUGen.Values = make([]uint, 0)

	for _, j := range jobs {
		traceCPUGen.Values = append(traceCPUGen.Values, uint(j.Cpus))
	}

	return traceCPUGen
}

func (ntg TraceCPUGen) SaveTraceCPUGen(filename string) error {
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

func LoadTraceCPUGen(filename string) (*TraceCPUGen, error) {
	ntg := &TraceCPUGen{}
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

func (ntg TraceCPUGen) CPUs() uint {
	return ntg.Sample()
}

type TraceDelayGen struct {
	Uint64Trace
}

func NewTraceDelayGen(jobs []*job.Job) TraceDelayGen {
	traceDelayGen := TraceDelayGen{}
	traceDelayGen.Values = make([]uint64, 0)

	for _, j := range jobs {
		traceDelayGen.Values = append(traceDelayGen.Values, uint64(j.Submission))
	}

	return traceDelayGen
}

func (ntg TraceDelayGen) SaveTraceDelayGen(filename string) error {
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

func LoadTraceDelayGen(filename string) (*TraceDelayGen, error) {
	ntg := &TraceDelayGen{}
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

func (ntg TraceDelayGen) Delay() uint64 {
	return ntg.Sample()
}

type TraceFileSelector struct {
	StringTrace
}

func NewTraceFileSelector(jobs []*job.Job) TraceFileSelector {
	traceFileSelector := TraceFileSelector{}
	traceFileSelector.Values = make([]string, 0)

	for _, j := range jobs {
		traceFileSelector.Values = append(traceFileSelector.Values, string(j.File))
	}

	return traceFileSelector
}

func (ntg TraceFileSelector) SaveTraceFileSelector(filename string) error {
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

func LoadTraceFileSelector(filename string) (*TraceFileSelector, error) {
	ntg := &TraceFileSelector{}
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

func (ntg TraceFileSelector) File() string {
	return ntg.Sample()
}
