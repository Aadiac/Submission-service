# Submission API + Flink hourly aggregation

Starter implementation for the live-coding assignment. Components: Go/Gin API, PostgreSQL, transactional outbox, Kafka, Flink Java DataStream job, and a Go load generator.

## 1. Requirements
- Docker Desktop with Compose
- Go 1.23+
- Java 11+ and Maven (to build the Flink job)
- Optional: curl/Postman

## 2. Build the Flink job first
The Compose file mounts the built JAR into the Flink JobManager, so build it before starting Compose:

```bash
cd flink
mvn clean package
cd ..
```

## 3. Start PostgreSQL, Kafka, API, and Flink
From the project root:

```bash
docker compose up --build -d
docker compose ps
curl http://localhost:8080/health
```

The database schema is mounted into the PostgreSQL initialization directory. If you already started an older database volume and changed the migration, reset it with `docker compose down -v` (this deletes local database data), then start again.

## 4. Test the API
Create a new submission:

```bash
curl -i -X POST http://localhost:8080/submissions \
  -H "Content-Type: application/json" \
  -d '{"form_id":"contact-form","idempotency_key":"abc-123","email":"user@example.com","data":{"name":"John","message":"Hello"}}'
```

Repeat the exact request. First request should return 201 and `"created":true`; repeat should return 200 and `"created":false"` with the same record.

Validation tests:
- Remove `form_id` -> 400
- Use `"email":"not-an-email"` -> 400

Pagination:

```bash
curl "http://localhost:8080/forms/contact-form/submissions?page=1"
```

Aggregation endpoint:

```bash
curl "http://localhost:8080/forms/contact-form/aggregations"
```

## 5. Run the 100,000-request load test
Run from a second terminal while the API is running:

```bash
go run ./loadtest -url http://localhost:8080 -n 100000 -workers 100
```

The generator creates 80,000 distinct idempotency keys and then sends 20,000 requests reusing those keys. It reports actual 201 responses, 200 duplicate responses, unexpected responses/errors, duration, and completed-request throughput. Do not put expected values in the report; use the measured output. For a repeatable clean run, reset the database volume first or change the key prefix.

## 6. Submit the Flink job
The Compose stack starts a Flink 1.20 JobManager and TaskManager. Open the Flink dashboard at `http://localhost:8081`, then submit the already-built JAR:

```bash
docker compose exec flink-jobmanager flink run -d \\
  -Dexecution.checkpointing.interval=10s \\
  /opt/flink/usrlib/submission-aggregation.jar
```

The job uses these defaults, which match the Compose service names:

- `KAFKA_BROKERS=kafka:29092`
- `KAFKA_TOPIC=submission-events`
- `JDBC_URL=jdbc:postgresql://postgres:5432/submissions`
- `DB_USER=app`
- `DB_PASSWORD=app`

The job uses one-hour event-time tumbling windows. It writes the absolute count for each `(form_id, hour_start)` using PostgreSQL `ON CONFLICT DO UPDATE`, making sink retries idempotent. The outbox publisher may publish an event twice if it crashes after Kafka accepted the event but before marking the outbox row published; the Flink keyed state deduplicates on `submission_id`. Flink checkpoints include source offsets and keyed state.

**Important production caveat:** This is a single-node development setup. For strong production guarantees, configure durable Flink checkpoint storage (e.g. S3/HDFS), high availability, Kafka replication, retention, monitoring, and validate recovery behavior. JDBC upsert is idempotent for the absolute window result, but the JDBC connector is not an XA exactly-once sink in this configuration. Explain this distinction honestly: checkpointing + source offsets + dedup state protect Flink's computation; idempotent upsert makes sink retries safe for the same window result.

## 7. Demonstrate aggregation
The job emits a window result when the event-time watermark passes the end of that hour. Because this uses event time, a newly submitted event for the current hour may not appear in the aggregation table until that hour closes (or the watermark advances). For a quick demo, use timestamps in a completed hour or wait for the hour boundary. Query:

```sql
SELECT form_id, hour_start, submission_count
FROM hourly_aggregations
ORDER BY hour_start DESC;
```

## 8. Suggested demo sequence
1. `docker compose up --build -d`
2. Create a submission twice; show 201 then 200.
3. Show invalid email and missing form_id return 400.
4. Show page 1 returns at most 10 rows.
5. Run the load test and save terminal output.
6. Build and submit Flink job; show hourly aggregation rows.
7. Explain outbox, unique idempotency constraint, Flink checkpointing, dedup state, event-time windows, and idempotent sink.

## API response behavior
- New record: HTTP 201, `created: true`
- Existing idempotency key: HTTP 200, `created: false`
- Validation failure: HTTP 400
- Unexpected database failure: HTTP 500

## Notes / limitations to address before production
- Idempotency key is globally unique in this assignment implementation. If keys should only be unique within a form, change the unique constraint to `(form_id, idempotency_key)` and use that pair consistently.
- The outbox publisher is intentionally simple and single-instance. Multiple publisher replicas should claim rows with `FOR UPDATE SKIP LOCKED` inside a transaction.
- The sample email validation uses Go's standard `net/mail` parser; production validation may require stricter policy.
- Add graceful shutdown, structured logging, metrics, request size limits, auth/rate limiting, and integration tests for a production service.
