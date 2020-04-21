package file

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dsfalves/simulator/topology"
)

type File struct {
	Size      uint64
	Locations []*topology.DataCenter
}

func Load(reader io.Reader, locations []*topology.DataCenter) (map[string]*File, error) {
	res := make(map[string]*File)

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, " ")
		s, err := strconv.ParseUint(words[1], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
		}
		f := &File{
			Size:      s,
			Locations: make([]*topology.DataCenter, 0, len(words)-2),
		}
		for i := 2; i < len(words); i++ {
			k, err := strconv.ParseUint(words[i], 0, 64)
			if err != nil {
				return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
			}
			f.Locations[i] = locations[k]
		}
		res[words[0]] = f
	}
	return res, nil
}
