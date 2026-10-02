include .env

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



#  docker exec -it postgres_container bash 
# psql -U myuser -d mydb