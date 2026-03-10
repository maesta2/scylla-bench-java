.PHONY: build package clean docker-build docker-run help version

# Default ScyllaDB Java driver version (override with: make build DRIVER_VERSION=4.18.0.0)
DRIVER_VERSION ?= 4.18.0.0
DOCKER_IMAGE   ?= scylla-bench-java
DOCKER_TAG     ?= latest
JAR            = target/scylla-bench-java.jar

## build: Compile and package the fat JAR
build:
	mvn package -DskipTests -Dscylla.driver.version=$(DRIVER_VERSION)

## package: Same as build (alias)
package: build

## clean: Remove build artifacts
clean:
	mvn clean

## test: Run unit tests
test:
	mvn test -Dscylla.driver.version=$(DRIVER_VERSION)

## version: Show the embedded version info from the built JAR
version: build
	java -jar $(JAR) --version

## run: Run with default arguments (shows help)
run: build
	java -jar $(JAR) --help

## docker-build: Build the Docker image
docker-build:
	docker build \
		--build-arg DRIVER_VERSION=$(DRIVER_VERSION) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):$(DRIVER_VERSION) \
		.

## docker-run: Run the Docker image (connects to localhost)
docker-run: docker-build
	docker run --rm --network=host $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-mode write -workload sequential -nodes 127.0.0.1 \
		-partition-count 1000 -clustering-row-count 10 \
		-concurrency 4 -duration 10s

## help: Show this help message
help:
	@grep -E '^## ' Makefile | sed 's/## //'
