package exporter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"

	"github.com/codeuniversity/xing-datahub-protocol"
)

// ItemExporter is responsible for handling the batching of Items
type ItemExporter struct {
	producer     sarama.AsyncProducer
	batchCount   int
	count        int
	maxBatchSize int
	fileHandle   *os.File
	filename     string
	filepath     string
}

// NewItemExporter initiliazes a ItemExporter
func NewItemExporter(batchSize int, producer sarama.AsyncProducer) *ItemExporter {
	if err := os.Mkdir(pathPrefix, os.ModePerm); err != nil {
		fmt.Println(err)
	}
	filename := "firstitems"
	filepath := pathPrefix + filename
	f, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	return &ItemExporter{
		producer:     producer,
		batchCount:   0,
		count:        0,
		maxBatchSize: batchSize,
		fileHandle:   f,
		filename:     filename,
		filepath:     filepath,
	}
}

//Export exports a Item
func (e *ItemExporter) Export(m *proto.Message) error {
	c := &protocol.Item{}
	code, err := proto.Marshal(*m)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(code, c); err != nil {
		return err
	}

	if err := e.writeToFile(c); err != nil {
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
func (e *ItemExporter) Commit() error {
	if e.batchCount == 0 {
		return nil
	}

	if err := e.fileHandle.Close(); err != nil {
		return err
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "items",
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
	e.filename = "items" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		return err
	}
	e.fileHandle = f
	return nil
}

func (e *ItemExporter) writeToFile(c *protocol.Item) error {
	s := itemToCsvLine(c)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		return err
	}
	if err := e.fileHandle.Sync(); err != nil {
		return err
	}
	return nil
}

func itemToCsvLine(i *protocol.Item) string {
	return fmt.Sprintf(
		"%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;\n",
		i.Id,
		i.Title,
		i.CareerLevel,
		i.DisciplineId,
		i.IndustryId,
		i.Country,
		i.IsPayed,
		i.Region,
		i.Latitude,
		i.Longitude,
		i.Employment,
		arrayHelper(i.Tags),
		i.CreatedAt,
	)
}
