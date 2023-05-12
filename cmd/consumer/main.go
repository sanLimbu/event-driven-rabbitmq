package main

import (
	"context"
	"event/internal"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("guest", "guest", "localhost:5672", "/")
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	publishConn, err := internal.ConnectRabbitMQ("guest", "guest", "localhost:5672", "/")
	if err != nil {
		panic(err)
	}

	defer publishConn.Close()

	client, err := internal.NewRabbitClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	publishClient, err := internal.NewRabbitClient(publishConn)
	if err != nil {
		panic(err)
	}
	defer publishClient.Close()

	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := client.CreateBinding(queue.Name, "", "data_events"); err != nil {
		panic(err)
	}

	messageBus, err := client.Consume(queue.Name, "email-service", false)
	if err != nil {
		panic(err)
	}

	var blocking chan struct{}
	//set a time for 15 secs
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	//errgroup allows concurrent task
	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			//spawn the worker
			msg := message
			g.Go(func() error {
				log.Printf("New message: %v\n ", msg)
				time.Sleep(10 * time.Second)
				if err := msg.Ack(false); err != nil {
					log.Println("Ack message failed")
					return err
				}

				if err := publishClient.Send(ctx, "data_callbacks", msg.ReplyTo, amqp091.Publishing{
					ContentType:   "text/plain",
					DeliveryMode:  amqp091.Persistent,
					Body:          []byte("RPC DONE"),
					CorrelationId: msg.CorrelationId,
				}); err != nil {
					panic(err)
				}
				log.Printf("Acknowledged message %s\n", msg.MessageId)
				return nil
			})

		}
	}()

	log.Println("consuming message")
	<-blocking

}
