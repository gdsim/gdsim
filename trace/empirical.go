package trace

import (
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/dsfalves/gdsim/job"
	"gonum.org/v1/gonum/stat/distuv"
)

// TraceGenerator contains all random generators required to generate a full trace.
type TraceGenerator struct {
	NTG  TraceNTG
	TDG  TraceTDG
	CGen TraceCPUGen
	DGen TraceDelayGen
	FSel TraceFileSelector
}

// TraceNTG is used to generate the number of tasks a job has based on existing job traces.
type TraceNTG struct {
	UintTrace
}

// NewTraceNTG creates a TraceNTG that follows the same distribution as in the jobs in a trace.
func NewTraceNTG(jobs []job.Job) TraceNTG {
	traceNTG := TraceNTG{}
	traceNTG.Values = make([]uint, 0)

	for _, j := range jobs {
		traceNTG.Values = append(traceNTG.Values, uint(len(j.Tasks)))
	}

	return traceNTG
}

// SaveTraceNTG saves the TraceNTG to a file named filename.
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

// LoadTraceNTG loads a TraceNTG that was saved in a file named filename.
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

// CreateNumTasks returns a value to be used as the number of tasks in a job.
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

// TraceTDG is used to generate the task duration for a synthetic job based on existing job traces.
type TraceTDG struct {
	Uint64Trace
}

// NewTraceTDG creates a TraceTDG that follows the same distribution as in the jobs in a trace.
func NewTraceTDG(jobs []job.Job) TraceTDG {
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

// TraceCPUGen is used to generate the number of cpu cores a job requires based on existing job traces.
type TraceCPUGen struct {
	UintTrace
}

func NewTraceCPUGen(jobs []job.Job) TraceCPUGen {
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

// TraceDelayGen is used to generate the delay after the previous job for the arrival of a job based on existing job traces.
type TraceDelayGen struct {
	Uint64Trace
}

func NewTraceDelayGen(jobs []job.Job) TraceDelayGen {
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

// TraceFileSelector is used to generate the number of tasks a job has based on existing job traces.
type TraceFileSelector struct {
	UintTrace
}

func NewTraceFileSelector(jobs []job.Job) TraceFileSelector {
	traceFileSelector := TraceFileSelector{}
	traceFileSelector.Values = make([]uint, 0)
	files := make(map[string]uint)

	for _, j := range jobs {
		id, present := files[j.File.Id()]
		if !present {
			id = uint(len(files))
			files[j.File.Id()] = id
		}
		traceFileSelector.Values = append(traceFileSelector.Values, id)
	}

	return traceFileSelector
}

func (tfs TraceFileSelector) SaveTraceFileSelector(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error opening %v: %v", filename, err)
	}
	enc := gob.NewEncoder(file)
	if err := enc.Encode(tfs); err != nil {
		return fmt.Errorf("error encoding %v: %v", filename, err)
	}
	return nil
}

func LoadTraceFileSelector(filename string) (*TraceFileSelector, error) {
	tfs := &TraceFileSelector{}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening %v: %v", filename, err)
	}
	dec := gob.NewDecoder(file)
	if err := dec.Decode(tfs); err != nil {
		return nil, fmt.Errorf("error decoding %v: %v", filename, err)
	}
	return tfs, nil
}

func (tfs TraceFileSelector) Compact(files []File) {
	max := uint(len(files))

	sort.Slice(tfs.Values, func(i, j int) bool { return tfs.Values[i] < tfs.Values[j] })
	cap := sort.Search(len(tfs.Values), func(i int) bool { return tfs.Values[i] >= max })
	tfs.Values = tfs.Values[:cap]
}

func (tfs TraceFileSelector) Size() uint {
	max := uint(0)
	for _, v := range tfs.Values {
		if v > max {
			max = v
		}
	}
	return max
}

func (tfs TraceFileSelector) File(files []File) File {
	sample := tfs.Sample()
	return files[sample]
}

type FileTraceGenerator struct {
	SizeGen     TraceSizeGenerator
	LocationSel TraceLocationSel
}

type TraceSizeGenerator struct {
	Uint64Trace
}

func NewTraceSG(files []File) TraceSizeGenerator {
	traceSG := TraceSizeGenerator{}
	traceSG.Values = make([]uint64, 0)

	for _, f := range files {
		traceSG.Values = append(traceSG.Values, f.Size())
	}

	return traceSG
}

func (ntg TraceSizeGenerator) SaveTraceSG(filename string) error {
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

func LoadTraceSG(filename string) (*TraceSizeGenerator, error) {
	ntg := &TraceSizeGenerator{}
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

func (ntg TraceSizeGenerator) Size() uint64 {
	return ntg.Sample()
}

type TraceLocationSel struct {
	Size UintTrace
	DCs  UintTrace
}

func NewTraceLS(files []File) TraceLocationSel {
	traceLS := TraceLocationSel{
		Size: UintTrace{make([]uint, 0)},
		DCs:  UintTrace{make([]uint, 0)},
	}
	dataCenters := make(map[uint]uint)
	count := uint(0)

	for _, f := range files {
		traceLS.Size.Values = append(traceLS.Size.Values, uint(len(f.Locations)))
		for _, l := range f.Locations {
			id, ok := dataCenters[l]
			if !ok {
				dataCenters[l] = count
				id = count
				count++
			}
			traceLS.DCs.Values = append(traceLS.DCs.Values, uint(id))
		}

	}

	return traceLS
}

func (ntg TraceLocationSel) SaveTraceLS(filename string) error {
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

func LoadTraceLS(filename string) (*TraceLocationSel, error) {
	ntg := &TraceLocationSel{}
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

func (sel TraceLocationSel) Locations() []uint {
	size := sel.Size.Sample()
	res := make([]uint, size)
	chosen := make(map[uint]int)
	for i := uint(0); i < size; i++ {
		// TODO: find a better way to do this selection
		next := sel.DCs.Sample()
		for _, ok := chosen[next]; ok; {
			next = sel.DCs.Sample()
		}
		chosen[next] = 1
		res[i] = next
	}
	return res
}
