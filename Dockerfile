# Multi-stage Dockerfile for Spring Boot Native Image
# Usage: docker build --build-arg SERVICE_PATH=choreography-saga/order-service --build-arg ARTIFACT_ID=choreography-order-service -t image-name .

# ============================================
# Stage 1: Build the native image using GraalVM
# ============================================
FROM ghcr.io/graalvm/graalvm-community:17 AS builder

# Install native-image
RUN gu install native-image

# Install Maven
ARG MAVEN_VERSION=3.9.6
RUN curl -fsSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar xzf - -C /opt \
    && ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/bin/mvn

WORKDIR /build

# Build arg for which service to build
ARG SERVICE_PATH
ARG ARTIFACT_ID

# Copy entire project (needed for multi-module build)
COPY . .

# Build native image in a single step:
# 1. Install parent pom and common module
# 2. Build the service with native profile (AOT + native-image)
# The skipNativeCompile property in parent pom prevents native compilation on pom/common modules
RUN mvn install -N -DskipTests -B && \
    mvn install -pl common -DskipTests -B && \
    mvn -pl ${SERVICE_PATH} -am package -Pnative -DskipTests -B

# ============================================
# Stage 2: Runtime image with native binary
# ============================================
FROM debian:bookworm-slim AS runtime

# Install curl for healthchecks and ca-certificates for HTTPS
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r appgroup && useradd -r -g appgroup appuser

# Set timezone
ENV TZ=UTC

WORKDIR /app

# Build args to locate the native binary
ARG SERVICE_PATH
ARG ARTIFACT_ID

# Copy native binary from builder
COPY --from=builder /build/${SERVICE_PATH}/target/${ARTIFACT_ID} /app/application

# Set ownership
RUN chown appuser:appgroup /app/application && chmod 755 /app/application

# Run as non-root user
USER appuser

# Expose default port (will be overridden by docker-compose)
EXPOSE 8080

# Health check using curl
HEALTHCHECK --interval=15s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${SERVER_PORT:-8080}/actuator/health || exit 1

# Run the native binary
ENTRYPOINT ["/app/application"]
