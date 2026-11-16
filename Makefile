.PHONY: build run vet test fmt

build:
	go build ./...

run:
	go run ./cmd/watcher

vet:
	go vet ./...

test:
	go test ./...

fmt:
	gofmt -s -w .

run-aggregator:
	go run ./cmd/aggregator
