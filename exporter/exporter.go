package exporter

import (
	"strconv"

	"github.com/golang/protobuf/proto"
)

var pathPrefix = "/datahub-data/"

// Exporter exports a proto message in batches
type Exporter interface {
	Export(*proto.Message) error
	Commit() error
}

func arrayHelper(arr []int32) string {
	s := ""
	for _, e := range arr {
		if len(s) != 0 {
			s += string('|')
		}
		s += strconv.Itoa(int(e))
	}
	return s
}
