.DEFAULT_GOAL := app
app:
	> .env
	docker compose down && docker compose up --build
lint:
	gofmt -w -s . && go mod tidy && go vet ./...
sim:
	# create env file:
	echo "SIMULATION=true" > .env
	docker compose down && docker compose up --build