package file

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dsfalves/gdsim/topology"
)

type File struct {
	id   string
	size uint64
}

func (f File) Id() string {
	return f.id
}

func (f File) Size() uint64 {
	return f.size
}

type FileContainer struct {
	files map[string]File
}

func (fc *FileContainer) Init() {
	fc.files = make(map[string]File)
}

func (fc FileContainer) Add(id string, data topology.Data) {
	f := data.(File)
	fc.files[id] = f
}

func (fc FileContainer) Has(id string) bool {
	_, ok := fc.files[id]
	return ok
}

func (fc FileContainer) Find(id string) topology.Data {
	return fc.files[id]
}

func (fc FileContainer) Pop(id string) topology.Data {
	f := fc.Find(id)
	delete(fc.files, id)
	return f
}

func Load(reader io.Reader, topo *topology.Topology) (map[string]File, error) {
	res := make(map[string]File)
	containers := make([]FileContainer, len(topo.DataCenters))
	for i := 0; i < len(containers); i++ {
		containers[i].Init()
		topo.DataCenters[i].Container = containers[i]
	}

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, " ")
		s, err := strconv.ParseUint(words[1], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
		}
		f := File{
			id:   words[0],
			size: s,
		}
		for i := 2; i < len(words); i++ {
			k, err := strconv.ParseInt(words[i], 0, 0)
			if err != nil {
				return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
			}
			//f.Locations[i-2] = int(k)
			containers[k].Add(f.id, f)
			topo.DataCenters[int(k)].Container.Add(f.Id(), f)
		}
		res[words[0]] = f
	}
	return res, nil
}
