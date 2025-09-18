.PHONY: run.up run.down logs.main connect-db logs.connect logs.connect-init client

run.up:
	docker compose up -d --build
run.down:
	docker compose down -v --remove-orphans

logs.main:
	docker compose logs -f main
logs.connect-init:
	docker compose logs -f kafka-connect-init
logs.connect:
	docker compose logs -f kafka-connect

connect-db:
	docker compose exec -it postgres psql -U postgres -d users

client:
	go run ./scripts/client/main.go

send-data:
	cd ./provided && ./events_producer.sh