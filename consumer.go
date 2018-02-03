package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/codeuniversity/xing-datahub-protocol"
	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"
	"github.com/codeuniversity/xing-datahub-consumer/exporter"
)

func main() {

	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, consumerConfig)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "users"
	// How to decide partition, is it fixed value...?
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, producerConfig)
	if err != nil {
		panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0
	exporter := exporter.NewUserExporter(2500, producer)
	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				user := &protocol.User{}
				proto.Unmarshal(msg.Value, user)
				exporter.ExportUser(user)
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}
