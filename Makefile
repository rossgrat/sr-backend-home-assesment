.PHONY: up down logs.main connect-db logs.connect logs.connect-init client data up.main

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