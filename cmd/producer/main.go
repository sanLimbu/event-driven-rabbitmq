package main

import (
	"context"
	"event/internal"
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("guest", "guest", "localhost:5672", "/")
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	consumeConn, err := internal.ConnectRabbitMQ("guest", "guest", "localhost:5672", "/")
	if err != nil {
		panic(err)
	}

	defer consumeConn.Close()

	client, err := internal.NewRabbitClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	consumeClient, err := internal.NewRabbitClient(consumeConn)
	if err != nil {
		panic(err)
	}
	defer consumeClient.Close()

	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "data_callbacks"); err != nil {
		panic(err)
	}

	messageBus, err := consumeClient.Consume(queue.Name, "data-api", true)
	if err != nil {
		panic(err)
	}

	go func() {
		for message := range messageBus {
			log.Printf("Message Callback %s\n", message.CorrelationId)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "data_events", "data.created.us", amqp091.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp091.Persistent,
			ReplyTo:       queue.Name,
			CorrelationId: fmt.Sprintf("customer created %d", i),
			Body:          []byte("Amazing message between services"),
		}); err != nil {
			panic(err)
		}

	}
	var blocking chan struct{}
	<-blocking
}
