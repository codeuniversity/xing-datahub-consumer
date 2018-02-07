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
func (e *TargetUserExporter) Export(m *proto.Message) {

	targetUser := &protocol.TargetUser{}
	code, err := proto.Marshal(*m)
	if err != nil {
		panic(err)
	}
	if err := proto.Unmarshal(code, targetUser); err != nil {
		panic(err)
	}

	e.batchCount++
	e.count++
	e.writeToFile(targetUser)
	if e.batchCount >= e.maxBatchSize {
		e.Commit()
	}
}

//Commit uploads the csv file prematurely
func (e *TargetUserExporter) Commit() {
	if e.batchCount == 0 {
		return
	}

	if err := e.fileHandle.Close(); err != nil {
		panic(err)
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "target_users",
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
	e.filename = "targetusers" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		panic(err)
	}
	e.fileHandle = f
}

func (e *TargetUserExporter) writeToFile(t *protocol.TargetUser) {
	s := targetUserToCsvLine(t)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		panic(err)
	}
	if err := e.fileHandle.Sync(); err != nil {
		panic(err)
	}
}

func targetUserToCsvLine(t *protocol.TargetUser) string {
	return fmt.Sprintf(
		"%v;\n",
		t.UserId,
	)
}
