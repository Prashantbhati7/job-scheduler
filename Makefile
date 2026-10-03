include .env
export

.PHONY: up down reset logs migrate-up migrate-down psql topics scheduler-test worker-test test

test:
	cd go-scheduler && go test -v ./...

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f

migrate-up:
	docker run --rm \
		--network host \
		-v "$(PWD)/migrations:/migrations" \
		migrate/migrate \
		-path=/migrations \
		-database "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable" \
		up

migrate-down:
	docker run --rm \
		--network host \
		-v "$(PWD)/migrations:/migrations" \
		migrate/migrate \
		-path=/migrations \
		-database "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable" \
		down 1

psql:
	docker exec -it postgres_container psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

topics:
	docker exec -it kafka_container /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

scheduler-test:
	cd go-scheduler && go run ./cmd/scheduler

#  docker exec -it postgres_container bash 
# psql -U myuser -d mydb

worker-test:
	cd go-scheduler && go run ./cmd/worker