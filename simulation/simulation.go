package simulation

import (
	"arkis_test/queue"
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

// Simulate a client inserting some messages in input queues and check results messages in output queues
func StartTestSimulation(ctx context.Context, inputs, outputs []queue.Queue) {
	// send some simulated message over to the input queues in background after processor starts properly:
	go func() {
		time.Sleep(3 * time.Second)
		for _, input := range inputs {
			err := input.Publish(ctx, []byte("Hello, World!"))
			if err != nil {
				log.WithError(err).Error("Main: Cannot publish message")
			}
		}
	}()

	// consume messages from output in backgroud
	go func() {
		// consume messages from the output queues and process them:
		for _, output := range outputs {
			log.Info("Main: Consuming messages")
			deliveries, err := output.Consume(ctx)
			if err != nil {
				log.WithError(err).Error("Main: Cannot consume messages")
			}
			// start receiving messages in background
			go func() {
				for delivery := range deliveries {
					log.WithFields(log.Fields{"delivery": string(delivery.Body), "queue": string(output.Name())}).Info("Main: Received a message")
				}
			}()
		}
	}()
}
