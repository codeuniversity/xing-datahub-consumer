package exporter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"

	"github.com/codeuniversity/xing-datahub-protocol"
)

// TargetUserExporter is responsible for handling the batching of TargetUsers
type TargetUserExporter struct {
	producer     sarama.AsyncProducer
	batchCount   int
	count        int
	maxBatchSize int
	fileHandle   *os.File
	filename     string
	filepath     string
}

// NewTargetUserExporter initiliazes a TargetUserExporter
func NewTargetUserExporter(batchSize int, producer sarama.AsyncProducer) *TargetUserExporter {
	if err := os.Mkdir(pathPrefix, os.ModePerm); err != nil {
		fmt.Println(err)
	}
	filename := "firsttargetusers"
	filepath := pathPrefix + filename
	f, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	return &TargetUserExporter{
		producer:     producer,
		batchCount:   0,
		count:        0,
		maxBatchSize: batchSize,
		fileHandle:   f,
		filename:     filename,
		filepath:     filepath,
	}
}

//Export exports a TargetUser
func (e *TargetUserExporter) Export(m *proto.Message) error {

	targetUser := &protocol.TargetUser{}
	code, err := proto.Marshal(*m)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(code, targetUser); err != nil {
		return err
	}

	if err := e.writeToFile(targetUser); err != nil {
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
func (e *TargetUserExporter) Commit() error {
	if e.batchCount == 0 {
		return nil
	}

	if err := e.fileHandle.Close(); err != nil {
		return err
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "target_users",
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
	e.filename = "targetusers" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		return err
	}
	e.fileHandle = f
	return nil
}

func (e *TargetUserExporter) writeToFile(t *protocol.TargetUser) error {
	s := targetUserToCsvLine(t)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		return err
	}
	if err := e.fileHandle.Sync(); err != nil {
		return err
	}
	return nil
}

func targetUserToCsvLine(t *protocol.TargetUser) string {
	return fmt.Sprintf(
		"%v;\n",
		t.UserId,
	)
}
