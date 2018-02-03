package exporter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/Shopify/sarama"

	"github.com/codeuniversity/xing-datahub-protocol"
)

// UserExporter is responsible for handling the batching of users
type UserExporter struct {
	producer     sarama.SyncProducer
	batchCount   int
	count        int
	maxBatchSize int
	fileHandle   *os.File
	filename     string
	filepath     string
}

var pathPrefix = "/tmp/datahub-data/"

// NewUserExporter initiliazes a UserExporter
func NewUserExporter(batchSize int, producer sarama.SyncProducer) *UserExporter {
	if err := os.Mkdir(pathPrefix, os.ModePerm); err != nil {
		println(err)
	}
	filename := "firstusers-10"
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

//ExportUser exports a User
func (e *UserExporter) ExportUser(user *protocol.User) {
	e.batchCount++
	e.count++
	e.writeUserToFile(user)
	if e.batchCount >= e.maxBatchSize {
		e.Commit()
	}
}

//Commit uploads the csv file prematurely
func (e *UserExporter) Commit() {
	if err := e.fileHandle.Close(); err != nil {
		panic(err)
	}
	m := &sarama.ProducerMessage{
		Topic: "written_files",
		Value: sarama.StringEncoder(e.filepath),
	}
	e.producer.SendMessage(m)
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

func (e *UserExporter) writeUserToFile(user *protocol.User) {
	s := userToCsvLine(user)
	if _, err := e.fileHandle.Write([]byte(s)); err != nil {
		panic(err)
	}
}

func userToCsvLine(user *protocol.User) string {
	return fmt.Sprintf(
		"'%v'; '%v'; '%v'; '%v';'%v';'%v';'%v';'%v';'%v'; \n",
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
			s += "|"
		}
		s += ("'" + e + "'")
	}
	return s
}

func addressHelper(a *protocol.Address) string {
	return fmt.Sprintf(
		"'%v'|'%v'|'%v'|'%v'",
		a.Country,
		a.Zipcode,
		a.City,
		a.Street,
	)
}

func companyHelper(c *protocol.User_Company) string {
	return fmt.Sprintf(
		"'%v'|'%v'",
		c.Title,
		c.Name,
	)
}
