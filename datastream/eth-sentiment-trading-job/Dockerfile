FROM eclipse-temurin:17

ENV JAVA_HOME=/opt/java \
    FLINK_HOME=/opt/flink \
    PATH=/opt/java/bin:/opt/flink/bin:$PATH \
    HOME=/tmp

RUN apt-get update && apt-get install -y --no-install-recommends bash && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink

COPY datastream/eth-sentiment-trading-job/target/eth-sentiment-trading-job-1.0-SNAPSHOT.jar \
     /opt/flink/jobs/eth-sentiment-trading-job.jar

EXPOSE 8081

CMD ["sh", "-c", "flink run-application -p 2 /opt/flink/jobs/eth-sentiment-trading-job.jar"]
