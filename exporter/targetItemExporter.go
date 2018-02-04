package exporter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"

	"github.com/codeuniversity/xing-datahub-protocol"
)

// TargetItemExporter is responsible for handling the batching of TargetItems
type TargetItemExporter struct {
	producer     sarama.AsyncProducer
	batchCount   int
	count        int
	maxBatchSize int
	fileHandle   *os.File
	filename     string
	filepath     string
}

// NewTargetItemExporter initiliazes a TargetItemExporter
func NewTargetItemExporter(batchSize int, producer sarama.AsyncProducer) *TargetItemExporter {
	os.Mkdir(pathPrefix, os.ModePerm)
	filename := "firsttargetitems"
	filepath := pathPrefix + filename
	f, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	return &TargetItemExporter{
		producer:     producer,
		batchCount:   0,
		count:        0,
		maxBatchSize: batchSize,
		fileHandle:   f,
		filename:     filename,
		filepath:     filepath,
	}
}

//Export exports a TargetItem
func (e *TargetItemExporter) Export(m *proto.Message) {

	targetItem := &protocol.TargetItem{}
	code, err := proto.Marshal(*m)
	if err != nil {
		panic(err)
	}
	if err := proto.Unmarshal(code, targetItem); err != nil {
		panic(err)
	}

	e.batchCount++
	e.count++
	e.writeToFile(targetItem)
	if e.batchCount >= e.maxBatchSize {
		e.Commit()
	}
}

//Commit uploads the csv file prematurely
func (e *TargetItemExporter) Commit() {
	if e.batchCount == 0 {
		return
	}

	if err := e.fileHandle.Close(); err != nil {
		panic(err)
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "target_items",
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
	e.filename = "targetitems" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		panic(err)
	}
	e.fileHandle = f
}

func (e *TargetItemExporter) writeToFile(t *protocol.TargetItem) {
	s := targetItemToCsvLine(t)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		panic(err)
	}
}

func targetItemToCsvLine(t *protocol.TargetItem) string {
	return fmt.Sprintf(
		"%v;\n",
		t.ItemId,
	)
}
