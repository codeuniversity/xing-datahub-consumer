package exporter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"

	"github.com/codeuniversity/xing-datahub-protocol"
)

// UserExporter is responsible for handling the batching of users
type UserExporter struct {
	producer     sarama.AsyncProducer
	batchCount   int
	count        int
	maxBatchSize int
	fileHandle   *os.File
	filename     string
	filepath     string
}

// NewUserExporter initiliazes a UserExporter
func NewUserExporter(batchSize int, producer sarama.AsyncProducer) *UserExporter {
	os.Mkdir(pathPrefix, os.ModePerm)
	filename := "firstusers"
	filepath := pathPrefix + filename
	f, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	return &UserExporter{
		producer:     producer,
		batchCount:   0,
		count:        0,
		maxBatchSize: batchSize,
		fileHandle:   f,
		filename:     filename,
		filepath:     filepath,
	}
}

//Export exports a User
func (e *UserExporter) Export(m *proto.Message) {

	user := &protocol.User{}
	code, err := proto.Marshal(*m)
	if err != nil {
		panic(err)
	}
	if err := proto.Unmarshal(code, user); err != nil {
		panic(err)
	}

	e.batchCount++
	e.count++
	e.writeToFile(user)
	if e.batchCount >= e.maxBatchSize {
		e.Commit()
	}
}

//Commit uploads the csv file prematurely
func (e *UserExporter) Commit() {
	if err := e.fileHandle.Close(); err != nil {
		panic(err)
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "users",
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
	e.filename = "users" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		panic(err)
	}
	e.fileHandle = f
}

func (e *UserExporter) writeToFile(user *protocol.User) {
	s := userToCsvLine(user)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		panic(err)
	}
}

func userToCsvLine(user *protocol.User) string {
	return fmt.Sprintf(
		"%v;%v;%v;%v;%v;%v;%v;%v;%v;\n",
		user.Id,
		user.FirstName,
		user.LastName,
		user.Gender,
		arrayHelper(user.Wants),
		arrayHelper(user.Haves),
		arrayHelper(user.Languages),
		addressHelper(user.BusinessAddress),
		companyHelper(user.PrimaryCompany),
	)
}

func arrayHelper(arr []string) string {
	s := ""
	for _, e := range arr {
		if len(s) != 0 {
			s += string('|')
		}
		s += e
	}
	return s
}

func addressHelper(a *protocol.Address) string {
	return fmt.Sprintf(
		"%s|%s|%s|%s",
		a.Country,
		a.Zipcode,
		a.City,
		a.Street,
	)
}

func companyHelper(c *protocol.User_Company) string {
	return fmt.Sprintf(
		"%s|%s",
		c.Title,
		c.Name,
	)
}
