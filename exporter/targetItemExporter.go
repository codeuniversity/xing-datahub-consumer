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
	if err := os.Mkdir(pathPrefix, os.ModePerm); err != nil {
		fmt.Println(err)
	}
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
func (e *TargetItemExporter) Export(m *proto.Message) error {

	targetItem := &protocol.TargetItem{}
	code, err := proto.Marshal(*m)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(code, targetItem); err != nil {
		return err
	}

	if err := e.writeToFile(targetItem); err != nil {
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
func (e *TargetItemExporter) Commit() error {
	if e.batchCount == 0 {
		return nil
	}

	if err := e.fileHandle.Close(); err != nil {
		return err
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "target_items",
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
	e.filename = "targetitems" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		return err
	}
	e.fileHandle = f
	return nil
}

func (e *TargetItemExporter) writeToFile(t *protocol.TargetItem) error {
	s := targetItemToCsvLine(t)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		return err
	}
	if err := e.fileHandle.Sync(); err != nil {
		return err
	}
	return nil
}

func targetItemToCsvLine(t *protocol.TargetItem) string {
	return fmt.Sprintf(
		"%v;\n",
		t.ItemId,
	)
}
