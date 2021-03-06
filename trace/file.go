package trace

import (
	"fmt"
	"math"
	"math/rand"
	"os"

	"github.com/dsfalves/gdsim/file"
	"gonum.org/v1/gonum/stat/distuv"
)

type File struct {
	Locations []uint
	file.File
}

type FileCreator struct {
	SizeGen     SizeGenerator
	LocationSel LocationSelector
}

type SizeGenerator interface {
	Size() uint64
}

type ParetoSizeGenerator struct {
	Pareto distuv.Pareto
}

func CreateParetoSizeGenerator() ParetoSizeGenerator {
	return ParetoSizeGenerator{distuv.Pareto{Xm: 1.259, Alpha: 2.7}}
}

func (gen ParetoSizeGenerator) Size() uint64 {
	return uint64(math.Trunc(gen.Pareto.Rand()))
}

type LocationSelector interface {
	Locations() []uint
}

type ZipfSingleLocationSelector struct {
	NDC  uint
	Zipf *rand.Zipf
}

func CreateZipfSLS(source rand.Source, nDC uint, skew float64) ZipfSingleLocationSelector {
	sel := ZipfSingleLocationSelector{
		NDC:  nDC,
		Zipf: rand.NewZipf(rand.New(source), skew, 1, uint64(nDC-1)),
	}
	return sel
}

func (sel ZipfSingleLocationSelector) Locations() []uint {
	location := sel.Zipf.Uint64()
	locations := make([]uint, 1)
	locations[0] = uint(location)
	return locations
}

type ZipfLocationSelector struct {
	NDC  uint
	Zipf *rand.Zipf
}

func CreateZipfLS(source rand.Source, nDC uint, skew float64) ZipfLocationSelector {
	sel := ZipfLocationSelector{
		NDC:  nDC,
		Zipf: rand.NewZipf(rand.New(source), skew, 1, uint64(nDC-1)),
	}
	return sel
}

func (sel ZipfLocationSelector) Locations() []uint {
	selected := sel.Zipf.Uint64() + 1
	locations := make([]uint, sel.NDC)
	for i := range locations {
		locations[i] = uint(i)
	}
	rand.Shuffle(len(locations), func(i, j int) { locations[i], locations[j] = locations[j], locations[i] })
	return locations[:selected]
}

func (fc FileCreator) New(source rand.Source, nDCs uint, id string) File {
	var f File
	f.File = file.New(id, fc.SizeGen.Size())
	f.Locations = fc.LocationSel.Locations()
	return f
}

func (f File) String() string {
	s := fmt.Sprintf("%v %v", f.Id(), f.Size())
	for _, l := range f.Locations {
		s = fmt.Sprintf("%v %v", s, l)
	}
	return s
}

func (fc FileCreator) CreateFiles(source rand.Source, total, nDCs uint) []File {
	res := make([]File, total)

	for i := range res {
		id := fmt.Sprintf("file%v", i+1)
		res[i] = fc.New(source, nDCs, id)
	}

	return res
}

func SaveFiles(filename string, data []File) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, obj := range data {
		fmt.Fprintf(f, "%v\n", obj)
	}

	return nil
}
