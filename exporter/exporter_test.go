package exporter

import (
	"bufio"
	"os"
	"testing"

	"github.com/codeuniversity/xing-datahub-consumer/models"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/codeuniversity/xing-datahub-protocol"
	"github.com/gogo/protobuf/proto"
	fuzz "github.com/google/gofuzz"
	. "github.com/smartystreets/goconvey/convey"
)

func TestExporter(t *testing.T) {
	random := fuzz.New().NilChance(0)
	Convey("The Exporter", t, func() {
		Convey("exports the right batch size", func() {
			pathPrefix = "tmp/"
			producer := mocks.NewAsyncProducer(t, sarama.NewConfig())
			exporter := NewExporter(2, producer, "users")

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
			pathPrefix = "tmp/"
			producer := mocks.NewAsyncProducer(t, sarama.NewConfig())
			exporter := NewExporter(5, producer, "users")
			lineCounter := 0
			countedChannel := make(chan struct{})
			producer.ExpectInputWithCheckerFunctionAndSucceed(func(b []byte) (e error) {
				fileInfo := &protocol.WrittenCSVInfo{}
				err := proto.Unmarshal(b, fileInfo)
				if err != nil {
					return err
				}
				p := fileInfo.Filepath
				f, err := os.Open(p)
				if err != nil {
					return err
				}

				scanner := bufio.NewScanner(bufio.NewReader(f))
				for scanner.Scan() {
					lineCounter++
				}
				countedChannel <- struct{}{}
				return nil
			})

			for i := 0; i < 5; i++ {
				user := &protocol.User{}
				random.Fuzz(user)
				m := &models.User{M: user}
				exporter.Export(m)
			}
			<-countedChannel
			So(lineCounter, ShouldEqual, 5)
		})
		Convey("Writes the records correctly", func() {
			pathPrefix = "tmp/"
			producer := mocks.NewAsyncProducer(t, sarama.NewConfig())
			exporter := NewExporter(1, producer, "users")
			lineChannel := make(chan string)
			producer.ExpectInputWithCheckerFunctionAndSucceed(func(b []byte) (e error) {
				fileInfo := &protocol.WrittenCSVInfo{}
				err := proto.Unmarshal(b, fileInfo)
				if err != nil {
					return err
				}
				p := fileInfo.Filepath
				f, err := os.Open(p)
				if err != nil {
					return err
				}
				scanner := bufio.NewScanner(bufio.NewReader(f))
				scanner.Scan()
				line := scanner.Text()
				lineChannel <- line

				return nil
			})

			user := &protocol.User{}
			random.Fuzz(user)
			m := &models.User{M: user}
			exporter.Export(m)

			lineText := <-lineChannel
			So(lineText+"\n", ShouldEqual, m.ToCSVLine())
		})
	})
}
