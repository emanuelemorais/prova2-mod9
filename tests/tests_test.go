package main

import (
	"context"
	"fmt"
	"testing"
	"time"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
)
var InsertedID interface{}

type MyDoc struct {
    Message    string    `bson:"message"`
}

func ReturnClient() *mongo.Client {

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://root:password@localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func TestKafkaPersistence(t *testing.T) {

	message := "Hello, Kafka from Go!"
	topic := "test_topic"

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("Erro ao criar consumidor Kafka: %v", err)
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{topic}, nil)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"client.id":         "go-producer",
	})
	if err != nil {
		t.Fatalf("Erro ao criar produtor Kafka: %v", err)
	}
	defer producer.Close()

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)

	producer.Flush(15 * 1000)

	msg, err := consumer.ReadMessage(-1)
	if err == nil {
		fmt.Printf("Mensagem recebida: %s\n", string(msg.Value))

		if string(msg.Value) != message {
			t.Fatalf("Mensagem recebida é diferente da mensagem original")

		} else {
			fmt.Print("Mensagem recebida é igual à mensagem original")

			client := ReturnClient()
			collection := client.Database("test").Collection("messages")
			insertResult, err := collection.InsertOne(context.TODO(), bson.M{"message": msg.Value})
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println("ID armazenado: ", insertResult.InsertedID)
			InsertedID = insertResult.InsertedID;
		}

	} else {
		t.Fatalf("Erro do consumidor Kafka: %v", err)
	}

	time.Sleep(5 * time.Second)

	client := ReturnClient()
	result:= client.Database("test").Collection("messages").FindOne(context.Background(), bson.M{"_id": InsertedID})

	elem := &MyDoc{}
	err = result.Decode(elem)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Mensagem retornada pelo mongo db:" + elem.Message)


}
