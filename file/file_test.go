package file

import (
	"strings"
	"testing"

	"github.com/dsfalves/gdsim/topology"
	"github.com/google/go-cmp/cmp"
)

func TestLoad(t *testing.T) {
	cap := [][2]int{
		{1, 2},
		{2, 1},
	}
	speed := [][]uint64{
		{0, 1},
		{1, 0},
	}
	topo, err := topology.New(cap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}

	sample := "f1 2 0\nf2 6 0 1"
	key := map[string]File{
		"f1": {
			id:   "f1",
			size: 2,
		},
		"f2": {
			id:   "f2",
			size: 6,
		},
	}
	reader := strings.NewReader(sample)

	files, err := Load(reader, topo)
	if err != nil {
		t.Errorf("expected no error for sample '%v', found '%v'", sample, err)
	}
	if len(files) != 2 {
		t.Errorf("wrong size for files created by Load: expected %v, found %v", 2, len(files))
	}
	if !cmp.Equal(key, files) {
		t.Errorf("expected generated files equal to %v, found %v", key, files)
	}
}
