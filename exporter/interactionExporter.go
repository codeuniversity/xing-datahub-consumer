package exporter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"

	"github.com/codeuniversity/xing-datahub-protocol"
)

// InteractionExporter is responsible for handling the batching of interactions
type InteractionExporter struct {
	producer     sarama.AsyncProducer
	batchCount   int
	count        int
	maxBatchSize int
	fileHandle   *os.File
	filename     string
	filepath     string
}

// NewInteractionExporter initiliazes a InteractionExporter
func NewInteractionExporter(batchSize int, producer sarama.AsyncProducer) *InteractionExporter {
	os.Mkdir(pathPrefix, os.ModePerm)
	filename := "firstinteractions"
	filepath := pathPrefix + filename
	f, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	return &InteractionExporter{
		producer:     producer,
		batchCount:   0,
		count:        0,
		maxBatchSize: batchSize,
		fileHandle:   f,
		filename:     filename,
		filepath:     filepath,
	}
}

//Export exports a Interaction
func (e *InteractionExporter) Export(m *proto.Message) {

	Interaction := &protocol.Interaction{}
	code, err := proto.Marshal(*m)
	if err != nil {
		panic(err)
	}
	if err := proto.Unmarshal(code, Interaction); err != nil {
		panic(err)
	}

	e.batchCount++
	e.count++
	e.writeToFile(Interaction)
	if e.batchCount >= e.maxBatchSize {
		e.Commit()
	}
}

//Commit uploads the csv file prematurely
func (e *InteractionExporter) Commit() {
	if err := e.fileHandle.Close(); err != nil {
		panic(err)
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "interactions",
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
	e.filename = "interactions" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		panic(err)
	}
	e.fileHandle = f
}

func (e *InteractionExporter) writeToFile(interaction *protocol.Interaction) {
	s := interactionToCsvLine(interaction)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		panic(err)
	}
}

func interactionToCsvLine(i *protocol.Interaction) string {
	return fmt.Sprintf(
		"%v;%v;%v;%v;\n",
		i.UserId,
		i.ItemId,
		i.InteractionType,
		i.CreatedAt,
	)
}
