package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChannel := make(chan kafka.Event)
	fmt.Println("hello")

	producer := NewKafkaProducer()
	Publish("mensagem-teste", "test-topic", producer, []byte("transfer"), deliveryChannel)
	// using go, golang will move function to another thread
	// go DeliveryReport(deliveryChannel)
	DeliveryReport(deliveryChannel)

	// will await 1000 miliseconds to finish the function
	// this will help the application await the producer to send the message
	producer.Flush(5000)

	// e := <-deliveryChannel
	// msg := e.(*kafka.Message)

	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println(("an error occured sending the message"))
	// } else {
	// 	fmt.Println("Message sent", msg.TopicPartition) // Message sent test-topic[2]@1
	// }
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:29092",
		// "delivery.timeout,ms":"0", // the max time that application must await until confirm that message was sent,
	}

	producer, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return producer
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChannel chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChannel)
	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChannel chan kafka.Event) {
	for e := range deliveryChannel {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("an error occurred")
			} else {
				fmt.Println("message sent", ev.TopicPartition)
			}
		}
	}
}
