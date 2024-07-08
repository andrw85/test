package queue

import (
	"context"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type Queue interface {
	Consume(ctx context.Context) (<-chan Delivery, error)
	Publish(ctx context.Context, msg []byte) error
	Name() string
}

type Delivery struct {
	Body    []byte
	handler amqp.Delivery
}

type queue struct {
	amqpConnection *amqp.Connection
	channel        *amqp.Channel
	name           string
}

func New(connectionURL, queueName string) (*queue, error) {
	conn, err := amqp.Dial(connectionURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}

	return &queue{conn, ch, queueName}, nil
}

func (queue *queue) Consume(ctx context.Context) (<-chan Delivery, error) {
	deliveries, err := queue.channel.ConsumeWithContext(
		ctx,
		queue.name,
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, err
	}

	out := make(chan Delivery)

	go func() {
		for {
			select {
			case delivery := <-deliveries:
				out <- Delivery{delivery.Body, delivery}
			case <-ctx.Done():
				close(out)
				return
			}
		}
	}()

	return out, nil
}

func (queue *queue) Publish(ctx context.Context, msg []byte) error {
	data := amqp.Publishing{
		DeliveryMode:    amqp.Transient,
		Timestamp:       time.Now(),
		Body:            msg,
		ContentEncoding: "application/json",
	}

	return queue.channel.PublishWithContext(ctx, "", queue.name, true, false, data)
}

func (queue *queue) Name() string {
	return queue.name
}

func CreateQueues(inputQueueNames, outputQueueNames []string) ([]Queue, []Queue) {

	// create input queues:
	var inputs []Queue
	for _, queueName := range inputQueueNames {
		inputQueue, err := New(os.Getenv("RABBITMQ_URL"), queueName)
		if err != nil {
			log.WithError(err).Panic("Cannot create input queue")
		}
		inputs = append(inputs, inputQueue)
	}

	// create output queues:
	var outputs []Queue
	for _, queueName := range outputQueueNames {
		outputQueue, err := New(os.Getenv("RABBITMQ_URL"), queueName)
		if err != nil {
			log.WithError(err).Panic("Cannot create input queue")
		}
		outputs = append(outputs, outputQueue)
	}
	// return tuple of inputs and outputs
	return inputs, outputs
}
