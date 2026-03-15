# Default driver version (can be overridden with DRIVER_VERSION build arg)
ARG DRIVER_VERSION=4.19.0.6
ARG JAVA_VERSION=21

# ---- Build stage ----
FROM maven:3.9-eclipse-temurin-${JAVA_VERSION} AS builder

ARG DRIVER_VERSION
WORKDIR /build

COPY pom.xml .
# Download dependencies first (layer caching)
RUN mvn dependency:go-offline -Dscylla.driver.version=${DRIVER_VERSION} -q

COPY src ./src
RUN mvn package -DskipTests -Dscylla.driver.version=${DRIVER_VERSION} -q

# ---- Runtime stage ----
FROM eclipse-temurin:${JAVA_VERSION}-jre-jammy AS production

LABEL org.opencontainers.image.title="scylla-bench-java"
LABEL org.opencontainers.image.description="ScyllaDB benchmarking tool using the ScyllaDB Java Driver"
LABEL org.opencontainers.image.source="https://github.com/maesta2/scylla-bench-java"

ENV TZ=UTC
WORKDIR /app

COPY --from=builder /build/target/scylla-bench-java.jar /app/scylla-bench-java.jar

ENTRYPOINT ["java", "-jar", "/app/scylla-bench-java.jar"]
CMD ["--help"]
