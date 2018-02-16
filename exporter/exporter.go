package exporter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/codeuniversity/xing-datahub-consumer/metrics"
	"github.com/codeuniversity/xing-datahub-consumer/models"
	protocol "github.com/codeuniversity/xing-datahub-protocol"
	"github.com/golang/protobuf/proto"
)

var pathPrefix = "/datahub-data/"
var filePrefix = "tmp_"

// Exporter exports a proto message in batches
type Exporter struct {
	producer     sarama.AsyncProducer
	batchCount   int
	count        int
	maxBatchSize int
	fileHandle   *os.File
	recordType   string
	filename     string
	filepath     string
}

// NewExporter initiliazes an Exporter
func NewExporter(batchSize int, producer sarama.AsyncProducer, recordType string) *Exporter {
	if err := os.Mkdir(pathPrefix, os.ModePerm); err != nil {
		fmt.Println(err)
	}
	filepath := pathPrefix + filePrefix + recordType
	f, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	return &Exporter{
		producer:     producer,
		batchCount:   0,
		count:        0,
		maxBatchSize: batchSize,
		fileHandle:   f,
		recordType:   recordType,
		filename:     filePrefix + recordType,
		filepath:     filepath,
	}
}

//Export exports a Model
func (e *Exporter) Export(m models.Model) error {
	if err := e.writeToFile(m); err != nil {
		return err
	}
	e.batchCount++
	e.count++

	if e.batchCount >= e.maxBatchSize {
		return e.Commit()
	}
	return nil
}

//Commit uploads the csv file prematurely
func (e *Exporter) Commit() error {
	if e.batchCount == 0 {
		return nil
	}
	if err := e.fileHandle.Close(); err != nil {
		return err
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: e.recordType,
	}
	m, err := proto.Marshal(csvInfo)
	if err != nil {
		return err
	}

	kafkaMessage := &sarama.ProducerMessage{
		Topic: "written_files",
		Value: sarama.ByteEncoder(m),
	}
	e.producer.Input() <- kafkaMessage
	fmt.Println("sent: ", e.filepath)

	e.batchCount = 0
	e.filename = filePrefix + e.recordType + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		return err
	}
	e.fileHandle = f
	metrics.BatchesExported.WithLabelValues(e.recordType).Inc()
	return nil
}

func (e *Exporter) writeToFile(m models.Model) error {
	s := m.ToCSVLine()
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		return err
	}
	if err := e.fileHandle.Sync(); err != nil {
		return err
	}
	return nil
}
