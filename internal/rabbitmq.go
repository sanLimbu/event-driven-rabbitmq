package internal

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	//The connection used by the client
	conn *amqp.Connection
	//Channel is used to process / Send messages
	ch *amqp.Channel
}

func ConnectRabbitMQ(username, password, host, vhost string) (*amqp.Connection, error) {

	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
}

func NewRabbitClient(conn *amqp.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
	}

	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, err
	}

	return RabbitClient{
		conn: conn,
		ch:   ch,
	}, nil
}

func (r RabbitClient) Close() error {
	return r.ch.Close()
}

//createQueue will create a new queue base on given cfgs
func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) (amqp.Queue, error) {
	q, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	if err != nil {
		return amqp.Queue{}, nil
	}
	return q, err
}

//CreateBind will bind the current channel to the given exchange using routinekey provided
func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

//Send is used to publish payload onto exchange with the given routingkey
func (rc RabbitClient) Send(ctx context.Context, exchange, routingkey string, options amqp.Publishing) error {
	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingkey,
		//Mandatory is used to determine if an error should be returned upon failure
		true,
		//immediate
		false,
		options,
	)
	if err != nil {
		return err
	}
	// confirmation.Wait()
	log.Println(confirmation.Wait())
	return nil
}

//Used to Consume message
func (rc RabbitClient) Consume(queue, consumer string, autoAct bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queue, consumer, autoAct, false, false, false, nil)
}
