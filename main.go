package main

import (
	"arkis_test/database"
	"arkis_test/processor"
	"arkis_test/queue"
	"arkis_test/simulation"
	"context"
	"os"

	log "github.com/sirupsen/logrus"
)

func main() {

	ctx := context.Background()

	// define queues names:
	var inputNames []string = []string{"A-input", "B-input", "C-input"}
	var outputNames []string = []string{"A-output", "B-output", "C-output"}

	// create input and output queues:
	var inputs, outputs []queue.Queue = queue.CreateQueues(
		inputNames,
		outputNames,
	)

	log.Info("Application is ready to run")

	// Hack for testing the application instead of implementing a seperate client...
	// if enviroment variable SIMULATION is set to true, start the simulation:
	if os.Getenv("SIMULATION") == "true" {
		log.Info("Main: Starting simulation")
		simulation.StartTestSimulation(ctx, inputs, outputs)
	}

	processor.New(inputs, outputs, database.D{}).Run(ctx)
}
