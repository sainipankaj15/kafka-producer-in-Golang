package main

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/vmihailenco/msgpack"
)

// type messageFormat struct {
// 	MessageField string
// }

func main() {
	// Configuration for the producer.
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "brokerName:Port", // e.g., "localhost:9092"
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		fmt.Printf("Error creating Kafka producer: %s\n", err)
		return
	}
	// Produce messages to a topic.
	// topic := "gokarna_load_test_topic"
	// topic := "gokarna_load_test_topic"
	topic := "Fyers_ka_order_response"
	var num int = 65
	// fmt.Println(Data)
	fmt.Println(fmt.Sprint(num))
	// Convert the map to a JSON string
	jsonString, err := json.Marshal(Data)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	fmt.Println(string(jsonString))

	// Serialize the message using msgpack
	messageBytes, err := msgpack.Marshal(&jsonString)
	if err != nil {
		fmt.Printf("Error serializing message: %s\n", err)
		return
	}

	fmt.Println(string(messageBytes))

	// key := "#positions"
	// keyByte,err := msgpack.Marshal(&key)
	// if err != nil {
	// 	fmt.Printf("Error serializing message: %s\n", err)
	// 	return
	// }

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          messageBytes,
		// Key:            keyByte,
	}

	err = producer.Produce(message, nil)
	if err != nil {
		fmt.Printf("Failed to produce message: %s\n", err)
	}
	var i int = 0
	for {
		err = producer.Produce(message, nil)
		if err != nil {
			fmt.Printf("Failed to produce message: %s\n", err)
		}
		i = i + 1

		if i == 1 {
			break
		}
	}

	producer.Flush(3 * 1000) // Wait for up to 15 seconds for any outstanding messages to be delivered

	// Close the producer when done.
	producer.Close()
}
