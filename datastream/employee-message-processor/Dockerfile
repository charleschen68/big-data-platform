FROM eclipse-temurin:17-jdk-arm64-alpine@sha256:a04722d572a3d77f30c2a37e7e7ab0f3a5b2e6c8d9e1f2a3b4c5d6e7f8a9b0c

ENV JAVA_HOME=/opt/java \
    FLINK_HOME=/opt/flink \
    PATH=/opt/java/bin:/opt/flink/bin:$PATH \
    HOME=/tmp

RUN apk add --no-cache bash

WORKDIR /opt/flink

COPY employee-message-processor/target/employee-message-processor-1.0-SNAPSHOT.jar \
     /opt/flink/jobs/employee-message-processor.jar

EXPOSE 8081

CMD ["sh", "-c", "flink run-application -p 2 /opt/flink/jobs/employee-message-processor.jar"]
