package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ReturnClient() *mongo.Client {

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://root:password@localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func main() {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topic := "qualidadeAr"
	consumer.SubscribeTopics([]string{topic}, nil)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Sinal de interrupção recebido: %v\n", sig)
			return
		default:
			msg, err := consumer.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Mensagem recebida: %s\n", string(msg.Value))

				client := ReturnClient()
				collection := client.Database("kafka").Collection("messages")
				_, err := collection.InsertOne(context.Background(), map[string]interface{}{
					"message": string(msg.Value),
				})
				if err != nil {
					log.Printf("Erro ao salvar a mensagem no MongoDB: %v\n", err)
				} else {
					fmt.Println("Mensagem salva com sucesso no MongoDB!")
				}
			} else {
				log.Printf("Erro do consumidor Kafka: %v (%v)\n", err, msg)
				break
			}
		}
	}
}
