package exporter

import (
	"github.com/golang/protobuf/proto"
)

var pathPrefix = "/tmp/datahub-data/"

// Exporter exports a proto message in batches
type Exporter interface {
	Export(*proto.Message)
}
