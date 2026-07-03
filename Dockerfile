FROM docker.io/gradle:8.12-jdk21 AS builder
WORKDIR /src
COPY build.gradle.kts settings.gradle.kts ./
RUN gradle --no-daemon -q dependencies > /dev/null
COPY src ./src
RUN gradle --no-daemon -q bootJar

FROM gcr.io/distroless/java21-debian12:nonroot
ARG GIT_SHA=unknown
ENV APP_VERSION=${GIT_SHA}
COPY --from=builder /src/build/libs/svc-batch.jar /app.jar
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app.jar"]
