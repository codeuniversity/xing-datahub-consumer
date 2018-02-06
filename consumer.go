package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/codeuniversity/xing-datahub-protocol"

	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"
	"github.com/codeuniversity/xing-datahub-consumer/exporter"
)

var brokers = []string{"localhost:9092"}

func main() {

	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = false
	producerConfig.Producer.Return.Errors = false

	producer, err := sarama.NewAsyncProducer(brokers, producerConfig)
	if err != nil {
		panic(err)
	}

	userExporter := exporter.NewUserExporter(50000, producer)
	user := &protocol.User{}
	go consume(userExporter, user, "users")

	itemExporter := exporter.NewItemExporter(50000, producer)
	item := &protocol.Item{}
	go consume(itemExporter, item, "items")

	interactionExporter := exporter.NewInteractionExporter(50000, producer)
	interaction := &protocol.Interaction{}
	go consume(interactionExporter, interaction, "interactions")

	targetUserExporter := exporter.NewTargetUserExporter(50000, producer)
	targetUser := &protocol.TargetUser{}
	go consume(targetUserExporter, targetUser, "target_users")

	targetItemExporter := exporter.NewTargetItemExporter(50000, producer)
	targetItem := &protocol.TargetItem{}
	go consume(targetItemExporter, targetItem, "target_items")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	<-signals
	fmt.Println("Interrupt is detected")
}

func consume(e exporter.Exporter, m proto.Message, topic string) {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Consumer.MaxWaitTime = 5 * time.Second

	master, err := sarama.NewConsumer(brokers, consumerConfig)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		timer := time.NewTimer(time.Second * 5)
		select {
		case <-timer.C:
			e.Commit()
		case err := <-consumer.Errors():
			fmt.Println(err)
		case msg := <-consumer.Messages():
			proto.Unmarshal(msg.Value, m)
			e.Export(&m)
		case <-signals:
			fmt.Println(topic, " shutting down")
		}
		timer.Stop()
	}

}
