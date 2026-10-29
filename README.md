# kafka-intro

A small project for learning Kafka with Spring Boot.

```
POST /api/messages ──► producer-service ──► topic "numbers" (3 partitions) ──┐
                     (classifies input)   topic "words"   (3 partitions) ──┤
                                                                           ▼
                            H2 (shared TCP server) ◄── worker-service (group "analysis-workers")
                                                       odd/even, vowel/consonant counts
```

- **producer-service** (port 8081): validates the input and sends integers to `numbers` and words to `words`. The **record key is the value itself**, so the same value always lands on the same partition.
- **worker-service** (port 8091): consumes both topics, analyses each record and stores the result. **The offset is committed only after the DB write succeeds** (`enable-auto-commit=false`, `ack-mode=manual_immediate`, `ack.acknowledge()` after the save). A failed record is retried 3 times and then sent to `numbers.DLT` / `words.DLT`. A redelivered record is detected by the unique `(kafka_topic, kafka_partition, kafka_offset)` constraint and skipped.

The two services share no code. They only agree on the JSON shape of `InputEvent`.

## Run it

```bash
docker compose up -d                          # Kafka :29092, H2 :9092 (console :8082), kafka-ui :8080
mvn -pl producer-service spring-boot:run      # terminal 1
mvn -pl worker-service spring-boot:run        # terminal 2
```

```bash
curl -s -XPOST localhost:8081/api/messages -H 'Content-Type: application/json' -d '{"value":"42"}'
curl -s -XPOST localhost:8081/api/messages -H 'Content-Type: application/json' -d '{"value":"apple"}'
curl -s -XPOST localhost:8081/api/messages -H 'Content-Type: application/json' -d '{"value":"he!!o"}'   # 400

curl -s localhost:8091/api/results/numbers
curl -s localhost:8091/api/results/words
```

- kafka-ui: http://localhost:8080 (topics, partitions, consumer group lag)
- H2 console: http://localhost:8082, JDBC URL `jdbc:h2:tcp://localhost:9092/./analysis`, user `sa`, empty password

## Experiments

1. **Keys → partitions:** post `apple` several times. The response always shows the same `partition`. Different words spread across partitions 0–2.
2. **Scale the workers:** start more instances with `PORT=8092 mvn -pl worker-service spring-boot:run` and `PORT=8093 ...`. In kafka-ui → Consumers → `analysis-workers`, watch the partitions rebalance. A 4th instance gets no partitions, because there are only 3 per topic.
3. **Scale the producer:** `PORT=8083 mvn -pl producer-service spring-boot:run`. Producers are stateless, so any number of them can run.
4. **Commit-after-write:** `docker compose stop h2`, then post a few inputs. The worker logs retries, then sends the records to the `*.DLT` topics, and only then commits their offsets. If you start H2 again (`docker compose start h2`) within the retry window, the record is stored instead.
5. **Catch-up:** stop every worker, post 20 inputs, and watch the lag grow in kafka-ui. Start one worker and watch it drain the lag from the last committed offset.
6. **Try a null key:** in `InputPublisher`, send with `kafkaTemplate.send(topic, event)` and compare how the records spread.

## Tests

```bash
mvn test
```
