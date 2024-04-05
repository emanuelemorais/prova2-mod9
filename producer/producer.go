package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
	"encoding/json"
)

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"client.id":         "go-producer",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	topic := "qualidadeAr"
	message := map[string]interface{}{
		"idSensor":     "sensor_001",
		"timestamp":    time.Now(),
		"tipoPoluente": "PM2.5",
		"nivel":        35.2,
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(jsonMessage),
	}, nil)

	producer.Flush(15 * 1000)

}