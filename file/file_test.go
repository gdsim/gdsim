package file

import (
	"github.com/google/go-cmp/cmp"
	"strings"
	"testing"
)

func TestLoad(t *testing.T) {
	sample := "f1 2 0\nf2 5 0 1"
	key := map[string]File{
		"f1": File{
			Size:      2,
			Locations: []int{0},
		},
		"f2": File{
			Size:      5,
			Locations: []int{0, 1},
		},
	}
	reader := strings.NewReader(sample)

	files, err := Load(reader)
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
