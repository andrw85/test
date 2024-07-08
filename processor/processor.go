package processor

import (
	"arkis_test/database"
	"arkis_test/queue"
	"context"
	"errors"

	"reflect"

	log "github.com/sirupsen/logrus"
)

type processor struct {
	inputs []queue.Queue
	// define slice of queues:
	outputs  []queue.Queue
	database database.Database
}

func New(inputs, outputs []queue.Queue, db database.Database) processor {
	return processor{inputs, outputs, db}
}

func (p processor) Run(ctx context.Context) (int, queue.Delivery, error) {
	var emptyQueue queue.Delivery

	// initialize slice with select cases
	cases := make([]reflect.SelectCase, len(p.inputs)+1)

	// add all channels to select cases
	for i, ch := range p.inputs {
		// call consume to get channel where deliveries are received:
		deliveries, err := ch.Consume(ctx)
		if err != nil {
			return -1, emptyQueue, err
		}
		// create the select case with the channel:
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(deliveries)}
	}

	// deal with context cancellation
	cases[len(p.inputs)] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}

	for {
		// select on a dynamic number of channels, will return the first channel that has a message
		chosen, delivery, ok := reflect.Select(cases)

		if !ok { // channel closed
			if ctx.Err() != nil {
				return -1, emptyQueue, ctx.Err()
			}
			return chosen, emptyQueue, errors.New("channel closed")
		}

		// call process with delivery and check if error returned:
		if err := p.process(ctx, chosen, delivery.Interface().(queue.Delivery)); err != nil {
			return -1, emptyQueue, err
		}
	}
}

func (p processor) process(ctx context.Context, queueIndex int, delivery queue.Delivery) error {

	log.WithFields(log.Fields{"delivery": string(delivery.Body), "queue": p.inputs[queueIndex].Name()}).Info("Processing the delivery")

	data, err := p.database.Get(delivery.Body)
	if err != nil {
		return err
	}

	log.WithField("result", string(data)).Info("Processed the delivery")

	return p.outputs[queueIndex].Publish(ctx, []byte(data))
}
