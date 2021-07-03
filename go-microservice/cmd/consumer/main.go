package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:29092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		fmt.Println("consumer error", err.Error())
	}

	topics := []string{"test-topic"}
	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1)

		if err != nil {
			fmt.Println("consumer error", err.Error())
		} else {

			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}

}
