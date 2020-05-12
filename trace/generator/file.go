package main

import (
	"fmt"
	"gonum.org/v1/gonum/stat/distuv"
	"math"
	"math/rand"
	"os"
)

type fakeFile struct {
	id        string
	size      uint64
	locations []uint
}

func newFile(source rand.Source, nDCs uint) fakeFile {
	var f fakeFile
	f.size = createFileSize(source)
	f.locations = selectLocations(source, nDCs)
	return f
}

func (f fakeFile) String() string {
	s := fmt.Sprintf("%v %v", f.id, f.size)
	for _, l := range f.locations {
		s = fmt.Sprintf("%v %v", s, l)
	}
	return s
}

func createFileSize(source rand.Source) uint64 {
	p := distuv.Pareto{Xm: 1.259, Alpha: 2.7}

	size := uint64(math.Trunc(p.Rand()))
	return size
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

func createFiles(source rand.Source, total, nDCs uint) []fakeFile {
	res := make([]fakeFile, total)

	for i := range res {
		res[i] = newFile(source, nDCs)
		res[i].id = fmt.Sprintf("file%v", i+1)
	}

	return res
}

func printFiles(filename string, data []fakeFile) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	for _, obj := range data {
		fmt.Fprintf(f, "%v\n", obj)
	}

	return nil
}
