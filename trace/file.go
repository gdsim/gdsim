package trace

import (
	"fmt"
	"gonum.org/v1/gonum/stat/distuv"
	"math"
	"math/rand"
	"os"
)

type File struct {
	id        string
	size      uint64
	locations []uint
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

func (fc FileCreator) New(source rand.Source, nDCs uint) File {
	var f File
	f.size = fc.SizeGen.Size()
	f.locations = selectLocations(source, nDCs)
	return f
}

func (f File) String() string {
	s := fmt.Sprintf("%v %v", f.id, f.size)
	for _, l := range f.locations {
		s = fmt.Sprintf("%v %v", s, l)
	}
	return s
}

func selectLocations(source rand.Source, nDC uint) []uint {
	var s float64 = 2
	z := rand.NewZipf(rand.New(source), s, 1, uint64(nDC-1))
	selected := z.Uint64() + 1
	locations := make([]uint, nDC)
	for i := range locations {
		locations[i] = uint(i)
	}
	rand.Shuffle(len(locations), func(i, j int) { locations[i], locations[j] = locations[j], locations[i] })
	return locations[:selected]
}

func (fc FileCreator) CreateFiles(source rand.Source, total, nDCs uint) []File {
	res := make([]File, total)

	for i := range res {
		res[i] = fc.New(source, nDCs)
		res[i].id = fmt.Sprintf("file%v", i+1)
	}

	return res
}

func SaveFiles(filename string, data []File) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	for _, obj := range data {
		fmt.Fprintf(f, "%v\n", obj)
	}

	return nil
}
