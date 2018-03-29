package exporter

import (
	"bufio"
	"io"
	"os"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/codeuniversity/xing-datahub-consumer/models"
	"github.com/codeuniversity/xing-datahub-protocol"
	fuzz "github.com/google/gofuzz"
	. "github.com/smartystreets/goconvey/convey"
	hdfs "github.com/vladimirvivien/gowfs"
)

func TestExporter(t *testing.T) {
	Convey("The Exporter", t, func() {
		random := fuzz.New().NilChance(0)
		Convey("exports the right batch size", func() {

			client := &mockHDFSClient{}
			pathPrefix = "tmp/"
			producer := mocks.NewAsyncProducer(t, sarama.NewConfig())
			exporter := NewExporter(2, producer, "users", client)

			for i := 0; i < 3; i++ {
				producer.ExpectInputAndSucceed()
				for j := 0; j < 2; j++ {
					user := &protocol.User{}
					random.Fuzz(user)
					m := &models.User{M: user}
					exporter.Export(m)
				}
			}
		})
		Convey("exports the correct amount of records", func() {
			lineCounter := 0

			checkerClient := &mockHDFSClient{
				onCreate: func(f io.Reader) {
					scanner := bufio.NewScanner(bufio.NewReader(f))
					for scanner.Scan() {
						lineCounter++
					}
				},
			}
			pathPrefix = "tmp/"
			producer := mocks.NewAsyncProducer(t, sarama.NewConfig())
			exporter := NewExporter(5, producer, "users", checkerClient)

			producer.ExpectInputAndSucceed()

			for i := 0; i < 5; i++ {
				user := &protocol.User{}
				random.Fuzz(user)
				m := &models.User{M: user}
				exporter.Export(m)
			}
			So(lineCounter, ShouldEqual, 5)
		})
		Convey("Writes the records correctly", func() {
			pathPrefix = "tmp/"
			lineText := ""
			checkerClient := &mockHDFSClient{
				onCreate: func(f io.Reader) {
					scanner := bufio.NewScanner(bufio.NewReader(f))
					scanner.Scan()
					lineText = scanner.Text()
				},
			}
			producer := mocks.NewAsyncProducer(t, sarama.NewConfig())
			exporter := NewExporter(1, producer, "users", checkerClient)
			producer.ExpectInputAndSucceed()

			user := &protocol.User{}
			random.Fuzz(user)
			m := &models.User{M: user}
			exporter.Export(m)

			So(lineText+"\n", ShouldEqual, m.ToCSVLine())
		})
	})
}

func BenchmarkExport(b *testing.B) {
	pathPrefix = "tmp/"
	exporter := basicMockProducer("users")
	user := &protocol.User{}
	random := fuzz.New().NilChance(0)
	random.Fuzz(user)
	m := &models.User{M: user}
	for n := 0; n < b.N; n++ {
		exporter.Export(m)
	}
}

type mockHDFSClient struct {
	onCreate func(io.Reader)
}

func (m *mockHDFSClient) Create(f io.Reader, _ hdfs.Path, _ bool, _ uint64, _ uint16, _ os.FileMode, _ uint) (bool, error) {
	if m.onCreate != nil {
		m.onCreate(f)
	}
	return true, nil
}

func basicMockProducer(recordType string) *Exporter {
	producer := &mocks.AsyncProducer{}
	mockClient := &mockHDFSClient{}
	return NewExporter(20000000, producer, recordType, mockClient)
}
