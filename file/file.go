package file

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type File struct {
	Size      uint64
	Locations []int
}

func Load(reader io.Reader) (map[string]File, error) {
	res := make(map[string]File)

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, " ")
		s, err := strconv.ParseUint(words[1], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
		}
		f := File{
			Size:      s,
			Locations: make([]int, len(words)-2),
		}
		for i := 2; i < len(words); i++ {
			k, err := strconv.ParseInt(words[i], 0, 0)
			if err != nil {
				return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
			}
			f.Locations[i-2] = int(k)
		}
		res[words[0]] = f
	}
	return res, nil
}
