package exporter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"

	"github.com/codeuniversity/xing-datahub-protocol"
)

// ConnectionExporter is responsible for handling the batching of connections
type ConnectionExporter struct {
	producer     sarama.AsyncProducer
	batchCount   int
	count        int
	maxBatchSize int
	fileHandle   *os.File
	filename     string
	filepath     string
}

// NewConnectionExporter initiliazes a ConnectionExporter
func NewConnectionExporter(batchSize int, producer sarama.AsyncProducer) *ConnectionExporter {
	os.Mkdir(pathPrefix, os.ModePerm)

	filename := "firstconnections"
	filepath := pathPrefix + filename
	f, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	return &ConnectionExporter{
		producer:     producer,
		batchCount:   0,
		count:        0,
		maxBatchSize: batchSize,
		fileHandle:   f,
		filename:     filename,
		filepath:     filepath,
	}
}

//Export exports a Connection
func (e *ConnectionExporter) Export(m *proto.Message) {
	c := &protocol.Connection{}
	code, err := proto.Marshal(*m)
	if err != nil {
		panic(err)
	}
	if err := proto.Unmarshal(code, c); err != nil {
		panic(err)
	}

	e.batchCount++
	e.count++
	e.writeToFile(c)
	if e.batchCount >= e.maxBatchSize {
		e.Commit()
	}
}

//Commit uploads the csv file prematurely
func (e *ConnectionExporter) Commit() {
	if err := e.fileHandle.Close(); err != nil {
		panic(err)
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "connections",
	}
	m, err := proto.Marshal(csvInfo)
	if err != nil {
		panic(err)
	}

	kafkaMessage := &sarama.ProducerMessage{
		Topic: "written_files",
		Value: sarama.ByteEncoder(m),
	}
	e.producer.Input() <- kafkaMessage
	fmt.Println("sent: ", e.filepath)

	e.batchCount = 0
	e.filename = "connections" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		panic(err)
	}
	e.fileHandle = f
}

func (e *ConnectionExporter) writeToFile(c *protocol.Connection) {
	s := connectionToCsvLine(c)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		panic(err)
	}
}

func connectionToCsvLine(c *protocol.Connection) string {
	return fmt.Sprintf(
		"%v;%v;\n",
		c.A,
		c.B,
	)
}
