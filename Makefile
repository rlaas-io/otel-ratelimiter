.PHONY: build test lint clean cover tidy all ocb-install ocb-build docker-build docker-run

# ---------- Component development ----------

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
	rm -rf otelcol-ratelimiter

tidy:
	go mod tidy

all: tidy build lint test

# ---------- Custom Collector distribution (ocb) ----------
# See: https://opentelemetry.io/docs/collector/extend/ocb/

OCB_VERSION ?= 0.147.0
OCB ?= ./ocb

# Download the OpenTelemetry Collector Builder binary.
ocb-install:
	@echo "Downloading ocb v$(OCB_VERSION)..."
	curl --proto '=https' --tlsv1.2 -fL -o $(OCB) \
		https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/cmd%2Fbuilder%2Fv$(OCB_VERSION)/ocb_$(OCB_VERSION)_$(shell go env GOOS)_$(shell go env GOARCH)
	chmod +x $(OCB)

# Build the custom Collector distribution using ocb.
ocb-build: ocb-install
	$(OCB) --config builder-config.yaml

# ---------- Docker ----------

DOCKER_IMAGE ?= otelcol-ratelimiter
DOCKER_TAG   ?= latest

docker-build:
	docker buildx build --load \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		--platform=linux/amd64 .

docker-run:
	docker run -it --rm \
		-p 4317:4317 -p 4318:4318 \
		-v $(PWD)/example/policies.json:/etc/otel/policies.json:ro \
		--name otelcol-ratelimiter \
		$(DOCKER_IMAGE):$(DOCKER_TAG)
