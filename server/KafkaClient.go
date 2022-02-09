package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaClient struct {
	Produce *kafka.Producer
}

func NewKafkaClient() (*KafkaClient, error) {
	kafkaClient := KafkaClient{}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	kafkaClient.Produce = p
	if err != nil {
		return nil, err
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return &kafkaClient, nil
}

func (client *KafkaClient) Send(topic *string, message *string) error {
	// Produce messages to topic (asynchronously)
	err := client.Produce.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: -1}, //kafka.PartitionAny},
		Value:          []byte(*message),
	}, nil)

	// Wait for message deliveries before shutting down
	//p.Flush(15 * 1000)

	return err
}

func (client *KafkaClient) Close() {
	client.Produce.Close()
}
