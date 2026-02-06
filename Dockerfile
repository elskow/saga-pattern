FROM container-registry.oracle.com/graalvm/native-image:25 AS builder

ARG MAVEN_VERSION=3.9.6
RUN curl -fsSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar xzf - -C /opt \
    && ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/bin/mvn

WORKDIR /build

ARG SERVICE_PATH
ARG ARTIFACT_ID

COPY . .

RUN mvn install -N -DskipTests -B && \
    mvn install -pl common -DskipTests -B && \
    mvn -pl ${SERVICE_PATH} -am package -Pnative -DskipTests -B

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r appgroup && useradd -r -g appgroup appuser

ENV TZ=UTC

WORKDIR /app

ARG SERVICE_PATH
ARG ARTIFACT_ID

COPY --from=builder /build/${SERVICE_PATH}/target/${ARTIFACT_ID} /app/application

RUN chown appuser:appgroup /app/application && chmod 755 /app/application

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${SERVER_PORT:-8080}/actuator/health || exit 1

ENTRYPOINT ["/app/application"]
