.PHONY: build test lint clean cover

build:
	go build ./...

test:
	go test -v -race ./...

cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

lint:
	golangci-lint run

clean:
	rm -f coverage.out coverage.html

tidy:
	go mod tidy

all: tidy build lint test
