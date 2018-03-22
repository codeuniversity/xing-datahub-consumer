package main

import (
	"fmt"
	"log"
	"net/http"

	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

	hdfs "github.com/vladimirvivien/gowfs"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/codeuniversity/xing-datahub-consumer/exporter"
	"github.com/codeuniversity/xing-datahub-consumer/metrics"
	"github.com/codeuniversity/xing-datahub-consumer/models"
)

var brokers = []string{"localhost:9092"}
var config = cluster.NewConfig()

func main() {

	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = false
	producerConfig.Producer.Return.Errors = true
	client, err := hdfs.NewFileSystem(hdfs.Configuration{Addr: "localhost:50070", User: "cloudera"})
	if err != nil {
		log.Fatal(err)
	}
	producer, err := sarama.NewAsyncProducer(brokers, producerConfig)
	if err != nil {
		panic(err)
	}
	initPrometheus()
	go serve()

	config.Consumer.Return.Errors = false
	config.Group.Return.Notifications = false
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	signalChannels := []chan struct{}{}
	for i := 0; i < 5; i++ {
		signalChannels = append(signalChannels, make(chan struct{}, 1))
	}

	userExporter := exporter.NewExporter(20000000, producer, "users", client)
	user := &models.User{}
	go consume(userExporter, user, "users", signalChannels[0])

	itemExporter := exporter.NewExporter(20000000, producer, "items", client)
	item := &models.Item{}
	go consume(itemExporter, item, "items", signalChannels[1])

	interactionExporter := exporter.NewExporter(20000000, producer, "interactions", client)
	interaction := &models.Interaction{}
	go consume(interactionExporter, interaction, "interactions", signalChannels[2])

	targetUserExporter := exporter.NewExporter(20000000, producer, "target_users", client)
	targetUser := &models.TargetUser{}
	go consume(targetUserExporter, targetUser, "target_users", signalChannels[3])

	targetItemExporter := exporter.NewExporter(20000000, producer, "target_items", client)
	targetItem := &models.TargetItem{}
	go consume(targetItemExporter, targetItem, "target_items", signalChannels[4])

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)

	<-signals
	fmt.Println("Interrupt is detected")
	shutdown(signalChannels)
}

func consume(e *exporter.Exporter, m models.Model, topic string, shutdownSignal chan struct{}) {
	consumer, err := cluster.NewConsumer(brokers, "batcher", []string{topic}, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

loop:
	for {
		timer := time.NewTimer(time.Second * 5)
		select {
		case <-timer.C:
			e.Commit()
		case err := <-consumer.Errors():
			panic(err)
		case msg, ok := <-consumer.Messages():
			if ok {
				if err := m.UnmarshalFrom(msg.Value); err != nil {
					consumer.MarkOffset(msg, "")
					fmt.Println(err)
					metrics.MessagesConsumed.WithLabelValues(topic, "false").Inc()
				} else if err = e.Export(m); err != nil {
					fmt.Println(err)
					e.Commit()
					panic(err)
				} else {
					consumer.MarkOffset(msg, "")
					metrics.MessagesConsumed.WithLabelValues(topic, "true").Inc()
				}
			}
		case <-shutdownSignal:
			e.Commit()
			fmt.Println(topic, " shutting down")
			break loop
		}
		timer.Stop()
	}
	shutdownSignal <- struct{}{}
}

func initPrometheus() {
	prometheus.MustRegister(metrics.MessagesConsumed)
	prometheus.MustRegister(metrics.BatchesExported)
	http.Handle("/metrics", prometheus.Handler())
}

func serve() {
	if err := http.ListenAndServe(":3001", nil); err != nil {
		panic(err)
	}
}

func shutdown(signalChannels []chan struct{}) {
	fmt.Println("Shutting down consumer gorutines")
	for _, c := range signalChannels {
		c <- struct{}{}
	}
	fmt.Println("Waiting for final Commits")
	for _, c := range signalChannels {
		<-c
	}
	fmt.Println("Shutting down")
}
