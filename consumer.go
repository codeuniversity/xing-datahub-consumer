package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

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

	producer, err := sarama.NewAsyncProducer(brokers, producerConfig)
	if err != nil {
		panic(err)
	}
	initPrometheus()
	go serve()

	config.Consumer.Return.Errors = false
	config.Group.Return.Notifications = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	userExporter := exporter.NewExporter(20000000, producer, "users")
	user := &models.User{}
	go consume(userExporter, user, "users")

	itemExporter := exporter.NewExporter(20000000, producer, "items")
	item := &models.Item{}
	go consume(itemExporter, item, "items")

	interactionExporter := exporter.NewExporter(20000000, producer, "interactions")
	interaction := &models.Interaction{}
	go consume(interactionExporter, interaction, "interactions")

	targetUserExporter := exporter.NewExporter(20000000, producer, "target_users")
	targetUser := &models.TargetUser{}
	go consume(targetUserExporter, targetUser, "target_users")

	targetItemExporter := exporter.NewExporter(20000000, producer, "target_items")
	targetItem := &models.TargetItem{}
	go consume(targetItemExporter, targetItem, "target_items")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	<-signals
	fmt.Println("Interrupt is detected")
}

func consume(e *exporter.Exporter, m models.Model, topic string) {
	consumer, err := cluster.NewConsumer(brokers, "batcher", []string{topic}, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
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
		case <-signals:
			e.Commit()
			fmt.Println(topic, " shutting down")
			break loop
		}
		timer.Stop()
	}

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
