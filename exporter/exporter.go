package exporter

import (
	"fmt"
	"io"
	"os"
	"strconv"

	hdfs "github.com/vladimirvivien/gowfs"

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
	hdfsClient   hdfsClient
}

type hdfsClient interface {
	Create(io.Reader, hdfs.Path, bool, uint64, uint16, os.FileMode, uint) (bool, error)
}

// NewExporter initiliazes an Exporter
func NewExporter(batchSize int, producer sarama.AsyncProducer, recordType string, client hdfsClient) *Exporter {
	filename := filePrefix + recordType
	filepath := pathPrefix + filename + "/" + filename

	if err := os.MkdirAll(pathPrefix+filename, os.ModePerm); err != nil {
		fmt.Println(err)
	}

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
		hdfsClient:   client,
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

//Commit uploads the csv file
func (e *Exporter) Commit() error {
	if e.batchCount == 0 {
		return nil
	}
	if err := e.fileHandle.Close(); err != nil {
		return err
	}
	fmt.Println("copying", e.filepath)

	err := copyToRemote(e.filepath, e.filepath, e.hdfsClient)
	if err != nil {
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
	e.filepath = pathPrefix + e.filename + "/" + e.filename
	if err := os.MkdirAll(pathPrefix+e.filename, os.ModePerm); err != nil {
		fmt.Println(err)
	}

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
	return nil
}

func copyToRemote(src string, dst string, client hdfsClient) error {
	local, err := os.Open(src)
	if err != nil {
		return err
	}
	defer local.Close()

	_, err = client.Create(
		local,
		hdfs.Path{Name: dst},
		true,
		0,
		0,
		0700,
		0,
	)
	return err
}
