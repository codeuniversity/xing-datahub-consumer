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
func (e *ItemExporter) Export(m *proto.Message) {
	c := &protocol.Item{}
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
func (e *ItemExporter) Commit() {
	if e.batchCount == 0 {
		return
	}

	if err := e.fileHandle.Close(); err != nil {
		panic(err)
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "items",
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
	e.filename = "items" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		panic(err)
	}
	e.fileHandle = f
}

func (e *ItemExporter) writeToFile(c *protocol.Item) {
	s := itemToCsvLine(c)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		panic(err)
	}
	if err := e.fileHandle.Sync(); err != nil {
		panic(err)
	}
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
