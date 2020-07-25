package file

import (
	"github.com/dsfalves/simulator/topology"
	"strings"
	"testing"
)

func equal(a, b []*topology.DataCenter) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func present(files map[string]File, name string, size uint64, locations []*topology.DataCenter) bool {
	f, ok := files[name]
	if !ok {
		return false
	}
	if f.Size != size {
		return false
	}
	if !equal(locations, f.Locations) {
		return false
	}
	return true
}

func TestLoad(t *testing.T) {
	dcs := []*topology.DataCenter{
		&topology.DataCenter{},
		&topology.DataCenter{},
	}
	sample := "f1 2 0\nf2 5 0 1"
	reader := strings.NewReader(sample)

	files, err := Load(reader, dcs)
	if err != nil {
		t.Errorf("expected no error for sample '%v', found '%v'", sample, err)
	}
	if len(files) != 2 {
		t.Errorf("wrong size for files created by Load: expected %v, found %v", 2, len(files))
	}
}
