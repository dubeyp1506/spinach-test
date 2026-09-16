.PHONY: up down migrate seed test lint run-api run-worker

up:
	docker compose up -d postgres redis

down:
	docker compose down

migrate:
	migrate -path migrations -database "$${DATABASE_URL:-postgres://martech:martech@localhost:5432/martech?sslmode=disable}" up

seed:
	go run ./cmd/seed

test:
	go test -race ./...

lint:
	gofmt -l . && go vet ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

build:
	go build ./...
