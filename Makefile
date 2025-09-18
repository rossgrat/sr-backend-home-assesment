.PHONY: up down logs.main connect-db logs.connect logs.connect-init client data up.main mac-install test test.concise mocks

up:
	docker compose up -d --build
down:
	docker compose down -v --remove-orphans
up.main:
	docker compose up -d --build main

logs.main:
	docker compose logs -f main
logs.connect-init:
	docker compose logs -f kafka-connect-init
logs.connect:
	docker compose logs -f kafka-connect

connect-db:
	docker exec -it postgres psql -U kafkauser -d kafkadb

client:
	go run ./scripts/client/main.go

data:
	cd ./provided && ./events_producer.sh

mocks:
	mockery

test:
	go test -v -count=1 ./...

test.concise:
	go test -count=1 ./...

mac-install:
	brew update
	brew upgrade
	brew install go
	brew install mockery
	brew install docker
	brew install docker --cask
	brew install golangci-lint