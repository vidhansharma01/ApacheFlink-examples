# Local Kafka Fraud Demo

Start Kafka:

```powershell
docker compose -f docker-compose.kafka.yml up -d
```

Note:
`bitnami/kafka:latest` was not pullable during setup, so the compose file uses the concrete Bitnami legacy tag `bitnamilegacy/kafka:4.0.0-debian-12-r10` instead.

Create topics:

```powershell
docker exec local-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec local-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Run the Spring Boot Flink job:

```powershell
mvn clean spring-boot:run
```

Produce sample transactions:

```powershell
docker exec -it local-kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transactions
```

Example messages:

```json
{"transactionId":"txn-1","userId":"user-101","amount":12000,"merchantId":"m-1","eventTime":"2026-04-04T12:00:00Z"}
{"transactionId":"txn-2","userId":"user-101","amount":61000,"merchantId":"m-1","eventTime":"2026-04-04T12:00:10Z"}
{"transactionId":"txn-3","userId":"user-202","amount":35000,"merchantId":"m-2","eventTime":"2026-04-04T12:00:15Z"}
{"transactionId":"txn-4","userId":"user-202","amount":36000,"merchantId":"m-2","eventTime":"2026-04-04T12:00:20Z"}
```

Consume alerts:

```powershell
docker exec -it local-kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud-alerts --from-beginning
```

Checkpoint/restart verification:

1. Let the job run long enough for at least one checkpoint to complete.
2. Stop the Spring Boot process.
3. Start the job again with the same consumer group.
4. Produce a new transaction and verify the job continues from the last committed checkpointed Kafka offsets rather than replaying already processed records...
