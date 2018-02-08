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
	if err := os.Mkdir(pathPrefix, os.ModePerm); err != nil {
		fmt.Println(err)
	}
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
func (e *UserExporter) Export(m *proto.Message) error {

	user := &protocol.User{}
	code, err := proto.Marshal(*m)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(code, user); err != nil {
		return err
	}

	if err := e.writeToFile(user); err != nil {
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
func (e *UserExporter) Commit() error {
	if e.batchCount == 0 {
		return nil
	}
	if err := e.fileHandle.Close(); err != nil {
		return err
	}

	csvInfo := &protocol.WrittenCSVInfo{
		Filename:   e.filename,
		Filepath:   e.filepath,
		RecordType: "users",
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
	e.filename = "users" + strconv.Itoa(e.count)
	e.filepath = pathPrefix + e.filename
	f, err := os.Create(e.filepath)
	if err != nil {
		return err
	}
	e.fileHandle = f
	return nil
}

func (e *UserExporter) writeToFile(user *protocol.User) error {
	s := userToCsvLine(user)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		return err
	}
	if err := e.fileHandle.Sync(); err != nil {
		return err
	}
	return nil
}

func userToCsvLine(u *protocol.User) string {
	return fmt.Sprintf(
		"%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;\n",
		u.Id,
		arrayHelper(u.Jobroles),
		u.CareerLevel,
		u.DisciplineId,
		u.IndustryId,
		u.Country,
		u.Region,
		u.ExperienceNEntriesClass,
		u.ExperienceYearsExperience,
		u.ExperienceYearsInCurrent,
		u.EduDegree,
		arrayHelper(u.EduFieldofstudies),
		u.Wtcj,
		u.Premium,
	)
}
