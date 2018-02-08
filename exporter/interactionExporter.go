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
	if err := os.Mkdir(pathPrefix, os.ModePerm); err != nil {
		fmt.Println(err)
	}
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
func (e *InteractionExporter) Export(m *proto.Message) error {

	Interaction := &protocol.Interaction{}
	code, err := proto.Marshal(*m)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(code, Interaction); err != nil {
		return err
	}

	if err := e.writeToFile(Interaction); err != nil {
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
func (e *InteractionExporter) Commit() error {
	if e.batchCount == 0 {
		return nil
	}

	if err := e.fileHandle.Close(); err != nil {
		return err
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "interactions",
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
	e.filename = "interactions" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		return err
	}
	e.fileHandle = f
	return nil
}

func (e *InteractionExporter) writeToFile(interaction *protocol.Interaction) error {
	s := interactionToCsvLine(interaction)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		return err
	}
	if err := e.fileHandle.Sync(); err != nil {
		return err
	}
	return nil
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
