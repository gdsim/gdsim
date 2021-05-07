package file

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dsfalves/gdsim/network"
	"github.com/dsfalves/gdsim/scheduler/event"
	"github.com/dsfalves/gdsim/topology"
)

type File struct {
	id   string
	size uint64
}

func New(id string, size uint64) File {
	return File{
		id:   id,
		size: size,
	}
}

func (f File) Id() string {
	return f.id
}

func (f File) Size() uint64 {
	return f.size
}

func (f File) Equal(d topology.Data) bool {
	return f.Id() == d.Id() && f.Size() == d.Size()
}

type SimpleFileDatabase map[string][]string

func (db SimpleFileDatabase) Location(id string) []string {
	return db[id]
}

type FileContainer struct {
	self  string
	files map[string]File
	db    topology.Database
	nw    network.Network
}

func (fc *FileContainer) SetDatabase(db topology.Database) {
	fc.db = db
}

func (fc *FileContainer) SetNetwork(nw network.Network) {
	fc.nw = nw
}

func (fc *FileContainer) Init(self string) {
	fc.self = self
	fc.files = make(map[string]File)
}

func (fc FileContainer) Add(id string, data topology.Data) {
	f := data.(File)
	fc.files[id] = f
}

// this should not care what location the scheduler used to estimate,
// it should find the best one and transfer from there
func (fc FileContainer) Transfer(when uint64, id string, data topology.Data, consequence func(time uint64) []event.Event) []event.Event {
	f := data.(File)
	if _, ok := fc.files[id]; !ok {
		best := ""
		var bestStatus network.LinkStatus
		for _, location := range fc.db.Location(id) {
			status := fc.nw.Status(fc.self, location)
			if best == "" || status.Bandwidth < bestStatus.Bandwidth {
				best = location
				bestStatus = status
			}
		}
		fc.nw.StartTransfer(when, f.size, best, fc.self, func(time uint64) []event.Event {
			fc.Add(id, data)
			return consequence(time)
		})
	} else {
		return consequence(when)
	}
	return nil
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
		containers[i].Init(fmt.Sprintf("FC_%d", i))
		topo.DataCenters[i].AddContainer(&containers[i])
	}

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, " ")
		s, err := strconv.ParseUint(words[1], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
		}
		f := New(words[0], s)
		for i := 2; i < len(words); i++ {
			k, err := strconv.ParseInt(words[i], 0, 0)
			if err != nil {
				return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
			}
			//f.Locations[i-2] = int(k)
			containers[k].Add(f.id, f)
			topo.DataCenters[int(k)].Container().Add(f.Id(), f)
		}
		res[words[0]] = f
	}
	return res, nil
}
