package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/codeuniversity/xing-datahub-protocol"

	"github.com/golang/protobuf/proto"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/codeuniversity/xing-datahub-consumer/exporter"
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

	config.Consumer.Return.Errors = false
	config.Group.Return.Notifications = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	userExporter := exporter.NewUserExporter(20000000, producer)
	user := &protocol.User{}
	go consume(userExporter, user, "users")

	itemExporter := exporter.NewItemExporter(20000000, producer)
	item := &protocol.Item{}
	go consume(itemExporter, item, "items")

	interactionExporter := exporter.NewInteractionExporter(20000000, producer)
	interaction := &protocol.Interaction{}
	go consume(interactionExporter, interaction, "interactions")

	targetUserExporter := exporter.NewTargetUserExporter(20000000, producer)
	targetUser := &protocol.TargetUser{}
	go consume(targetUserExporter, targetUser, "target_users")

	targetItemExporter := exporter.NewTargetItemExporter(20000000, producer)
	targetItem := &protocol.TargetItem{}
	go consume(targetItemExporter, targetItem, "target_items")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	<-signals
	fmt.Println("Interrupt is detected")
}

func consume(e exporter.Exporter, m proto.Message, topic string) {
	consumer, err := cluster.NewConsumer(brokers, "batcher", []string{topic}, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		timer := time.NewTimer(time.Second * 5)
		select {
		case <-timer.C:
			e.Commit()
		case err := <-consumer.Errors():
			fmt.Println(err)
		case msg, ok := <-consumer.Messages():
			if ok {
				if err := proto.Unmarshal(msg.Value, m); err != nil {
					fmt.Println(err)
					consumer.MarkOffset(msg, "")
				} else if err = e.Export(&m); err != nil {
					fmt.Println(err)
					e.Commit()
					panic(err)
				} else {
					consumer.MarkOffset(msg, "")
				}

			}
		case <-signals:
			e.Commit()
			fmt.Println(topic, " shutting down")
		}
		timer.Stop()
	}

}
