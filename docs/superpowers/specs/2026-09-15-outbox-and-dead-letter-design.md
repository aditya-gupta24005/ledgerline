# Phase 2A: Transactional Outbox and Dead-Letter Handling

- **Status:** Approved in design review, 2026-09-15
- **Scope:** `order-service`, `settlement-service`, `docker-compose.yml`, README
- **Sub-project order for phase 2:** **A (this document)** → C risk and P&L → B Keycloak → D dashboard

## 1. Problem

Two failure modes lose or stall trades today:

1. **Kafka unavailable while matching.** `KafkaTradeEventPublisher` sends asynchronously from the engine thread
   and only logs failures. A trade the client saw as `FILLED` (HTTP 201) may never reach settlement.
2. **Poison messages in settlement.** `DefaultErrorHandler(FixedBackOff(1s, 3))` logs and skips a record that
   keeps failing. The trade silently goes missing from the ledger, with no way to inspect or retry it.

## 2. Goals and non-goals

**Goals**

- A trade is committed to durable storage before the client is told it happened. Every committed trade
  eventually reaches Kafka, in match order per symbol, even if Kafka is down when it is matched.
- Settlement retries transient failures, sends permanent failures to a dead-letter topic (DLT) with
  diagnostics, and gives operators an API to inspect and replay dead-lettered records.
- Settlement's end result stays exactly once (already guaranteed by trade-id idempotency).

**Non-goals (explicitly deferred)**

- Rebuilding the order book after an order-service restart (journal and replay, phase 4). Resting orders stay
  in memory only.
- Deleting old published outbox rows.
- Securing the ops API (sub-project B adds the `ops` role).
- Grafana dashboards (phase 3; this sub-project only exposes metrics).
- Batched commits or Debezium CDC (considered and rejected for now, see §9).

## 3. Architecture

```
HTTP ──► OrderController ──► OrderGateway (engine thread)
                               1. engine.submit()
                               2. OutboxWriter.append(trades)  ── one tx ──► order_service.outbox_events
                               3. reply 201
                                                                               │
OutboxRelay (@Scheduled, advisory lock) ◄──────────────────────────────────────┘
   │ send in id order, wait for broker ack, mark published
   ▼
Kafka: ledgerline.trades.executed ──► TradeListener (settlement)
                                        │ parse JSON, SettlementService.settle()
                                        │ transient error: retry 1s → 2s → 4s
                                        │ permanent error or retries exhausted
                                        ▼
                                  Kafka: ledgerline.trades.executed.DLT ◄── Ops API: list and replay
```

## 4. order-service

### 4.1 Storage

- Uses the existing PostgreSQL database `ledgerline`, schema **`order_service`** (settlement keeps `public`).
  Each service owns its own schema and Flyway history.
- New dependencies: `spring-boot-starter-jdbc`, `spring-boot-starter-flyway`, `flyway-database-postgresql`,
  `postgresql` (runtime).
- Configuration:
  - `spring.datasource.url=jdbc:postgresql://localhost:5432/ledgerline?currentSchema=order_service`
  - `spring.flyway.schemas=order_service`, `spring.flyway.default-schema=order_service`

Migration `V1__outbox.sql`:

```sql
CREATE TABLE outbox_events (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic         VARCHAR(128) NOT NULL,
    message_key   VARCHAR(16)  NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL,
    published_at  TIMESTAMPTZ,
    attempts      INT          NOT NULL DEFAULT 0,
    last_error    TEXT
);

CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
```

`id` order is publish order. Rows are inserted only from the single engine thread, so id order equals match
order.

### 4.2 Write path (`OrderGateway`, on the engine thread)

For `submit(request)`:

1. If `halted`, throw `EngineHaltedException` (see 4.3) without touching the engine.
2. `MatchResult result = engine.submit(request)`.
3. Map trades to `TradeExecuted` events (unchanged mapping, including `T-<session>-<seq>` trade ids).
4. If there is at least one event, call `OutboxWriter.append(events)`: a single transaction (`TransactionTemplate`)
   batch-inserting one row per event with `topic=ledgerline.trades.executed`, `message_key=symbol`,
   `event_type=TradeExecuted`, and `payload` serialised with the application's Jackson `JsonMapper`.
5. Return `OrderOutcome`. The controller replies 201 as today.

Orders without trades write nothing.

`TradeEventPublisher` and `KafkaTradeEventPublisher` are deleted. The engine thread no longer uses Kafka, and
the `max.block.ms` producer override is removed.

### 4.3 Halt on persistence failure

If `OutboxWriter.append` throws, the in-memory book has already changed but the trades are not durable. The
gateway then:

- sets `halted = true` and records the cause and timestamp,
- throws `EngineHaltedException` for the current request.

While halted:

- `submit` and `cancel` throw `EngineHaltedException` immediately.
- `book(symbol, depth)` keeps working, since it is read-only.
- `ApiExceptionHandler` maps `EngineHaltedException` to **503**, `ProblemDetail` title `Engine halted`, detail
  `Order entry halted after a persistence failure; restart required`.
- `EngineHealthIndicator` reports `DOWN` with details `{reason, haltedAt}`, so `/actuator/health` becomes 503.

Only a restart clears the halt. There is no automatic un-halt.

### 4.4 Relay (`OutboxRelay`)

- `@Scheduled(fixedDelayString = "${ledgerline.outbox.relay.poll-interval:200ms}")`, with `@EnableScheduling` on the
  application.
- Uses its own `KafkaTemplate<String, String>` (`StringSerializer` for key and value, `acks=all`,
  `enable.idempotence=true`).

Each run, inside one transaction:

1. `SELECT pg_try_advisory_xact_lock(7401001)`. If false, return: another instance is relaying. This keeps
   one active relay across instances, which per-symbol ordering depends on.
2. `SELECT id, topic, message_key, payload::text FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT :batchSize`
   (`ledgerline.outbox.relay.batch-size`, default 100).
3. For each row in order: `kafkaTemplate.send(ProducerRecord(topic, key, payload) + header eventType)`, then
   `.get(ledgerline.outbox.relay.send-timeout, default 5s)`.
   - On success, remember the id.
   - On the **first** failure: `UPDATE outbox_events SET attempts = attempts + 1, last_error = :message WHERE id = :id`,
     then stop processing this batch.
4. `UPDATE outbox_events SET published_at = now() WHERE id = ANY(:publishedIds)`.
5. Commit, then update metrics.

Delivery is at least once. A crash between the broker ack and the commit republishes those rows on the next run;
settlement's idempotency absorbs the duplicates.

**Metrics** (Micrometer):

| Name | Type | Meaning |
|---|---|---|
| `ledgerline.outbox.pending` | Gauge | Value cached by the relay after each run: `count(*) WHERE published_at IS NULL` |
| `ledgerline.outbox.published` | Counter | Rows sent to Kafka |
| `ledgerline.outbox.publish.failures` | Counter | Failed send attempts |

`management.endpoints.web.exposure.include` stays `health,info,metrics`.

## 5. settlement-service

### 5.1 Consuming as String

- Consumer value deserializer changes to `StringDeserializer`. The `ErrorHandlingDeserializer` and `spring.json.*`
  properties are removed.
- `TradeListener.onTradeExecuted(ConsumerRecord<String, String> record)` parses with `JsonMapper.readValue(payload, TradeExecuted.class)`.
  Any parse or constructor-validation failure (`JacksonException`, `IllegalArgumentException`,
  `NullPointerException`) is wrapped in **`InvalidTradeMessageException`**.
- The wire contract is plain JSON plus the `eventType` header. No Java class names cross service boundaries.

### 5.2 Error handling

`DefaultErrorHandler(DeadLetterPublishingRecoverer, ExponentialBackOffWithMaxRetries(3) {initial 1s, multiplier 2})`:

| Exception | Classification |
|---|---|
| `InvalidTradeMessageException` | Not retryable: sent to the DLT immediately |
| `IllegalStateException` (for example unbalanced postings) | Not retryable: sent to the DLT immediately |
| Anything else (for example `TransientDataAccessException`, `CannotCreateTransactionException`, a duplicate-key race) | Retried 3 times (1s, 2s, 4s), then sent to the DLT |

- `DeadLetterPublishingRecoverer` uses a `KafkaTemplate<String, String>` (settlement gains producer config) and
  resolves the destination to `TopicPartition(record.topic() + ".DLT", record.partition())`.
- Standard Spring headers are kept: exception FQCN, message, stacktrace, and original topic, partition, offset and timestamp.
- `NewTopic` bean: `ledgerline.trades.executed.DLT`, 3 partitions, replication factor 1.

### 5.3 Ops API (`DeadLetterController`, backed by `DeadLetterReader` and `DeadLetterReplayer`)

**`GET /api/v1/ops/dead-letters?limit=50`**

- `limit` is 1–500 (clamped), default 50.
- Creates a short-lived consumer from the `ConsumerFactory` with no group id and `enable.auto.commit=false`, assigns all
  DLT partitions, and for each partition seeks to `max(beginning, end - limit)` and reads to the end offset.
- Merges records newest first by record timestamp, truncates to `limit`, and closes the consumer.
- Response `200`, a JSON array of:
  `{partition, offset, key, payload, exceptionClass, exceptionMessage, originalPartition, originalOffset, failedAt}`.
  `payload` is the raw string; `failedAt` is an ISO-8601 instant.

**`POST /api/v1/ops/dead-letters/{partition}/{offset}/replay`**

- `400` if `partition` does not exist on the DLT; `404` if no record exists at that offset.
- Republishes `(key, payload)` to `ledgerline.trades.executed` with header
  `ledgerline-replayed-from: ledgerline.trades.executed.DLT/<partition>/<offset>`, waiting for the broker ack.
- Response `202` with `{replayedTo: "ledgerline.trades.executed", partition, offset}`.
- Replaying is safe to repeat because `SettlementService.settle` ignores trade ids it has already booked.

**Known limitation:** Kafka records are immutable, so the list also shows already-replayed records. Tracking
replay state would need a database table, which was considered and declined.

## 6. Configuration and infrastructure changes

- `order-service/application.yml`: datasource, Flyway schema, and relay properties; producer switched to
  `StringSerializer`; `max.block.ms` removed.
- `settlement-service/application.yml`: `StringDeserializer`, producer section for the DLT and replay.
- `docker-compose.yml`: no changes; the existing Postgres and Kafka are reused.
- README: architecture diagram, design decisions, and the two demo scripts from §7.3.

## 7. Testing

### 7.1 order-service

| Test | Kind | Asserts |
|---|---|---|
| `OutboxWriterIT` | Testcontainers Postgres | Batch rows are inserted in order with correct topic, key and type; payload JSON round-trips to an equal `TradeExecuted`; a failing insert leaves no partial rows |
| `OrderGatewayTest` | Unit, failing fake `OutboxWriter` | First failure throws `EngineHaltedException`; later `submit` and `cancel` are rejected without reaching the engine; `book` still works; health indicator reports `DOWN` |
| `OutboxRelayIT` | Testcontainers Postgres and Kafka | Rows for several symbols are published; a consumer receives them in id order per key; rows are marked published; the pending gauge drops to 0 |
| `OutboxRelayFailureIT` | Postgres, mocked `KafkaTemplate` failing on the 3rd send | Rows 1–2 published; rows 3 and later pending; row 3 has `attempts = 1` and `last_error`; the next run with a healthy template publishes from row 3 in order |
| `OutboxRelayLockIT` | Postgres, two concurrent runs | Only one run sends; nothing is published twice |
| `OrderControllerTest` (updated) | MockMvc | Mocks `OutboxWriter` instead of the publisher; a crossing order appends one event; halted gateway gives 503 `Engine halted` |

### 7.2 settlement-service

| Test | Kind | Asserts |
|---|---|---|
| `TradeListenerTest` | Unit | Invalid JSON and invalid fields throw `InvalidTradeMessageException`; valid JSON calls `settle` |
| `DeadLetterIT` | Testcontainers Kafka and Postgres | A malformed record lands on the DLT on the same partition, with no retries (arrives in < 1s) and with exception headers |
| `TransientRetryIT` | Kafka, `@MockitoBean SettlementService` throwing `TransientDataAccessResourceException` twice, then succeeding | Record processed and nothing on the DLT |
| `DeadLetterOpsIT` | Kafka and Postgres | `GET` lists a dead-lettered record with exception fields; a valid trade placed on the DLT and replayed via `POST` gets booked; replaying again books nothing new; unknown offset gives 404 |
| Existing `SettlementServiceIT`, unit tests | unchanged | Still green |

Kafka containers use `apache/kafka:4.2.1` (same as Compose) with `@ServiceConnection`. Tests that need Docker
use `@Testcontainers(disabledWithoutDocker = true)`, and CI runs all of them.

### 7.3 Manual demo scripts (README)

1. **Kafka outage:** `docker compose stop kafka`, place crossing orders (201s returned), and see
   `/actuator/metrics/ledgerline.outbox.pending` > 0. Run `docker compose start kafka`; the pending count returns
   to 0 and balances appear in settlement in match order.
2. **Poison message:** produce `{"bad":` to `ledgerline.trades.executed` with `kafka-console-producer`; it appears
   in `GET /api/v1/ops/dead-letters`. Produce a valid trade JSON to the DLT and replay it via the API; balances update.

## 8. Acceptance criteria

1. With Kafka stopped, crossing orders still return 201 and their trades are in `outbox_events` with
   `published_at IS NULL`.
2. Within 10 seconds of Kafka becoming reachable again, every pending trade is published in id order and settles
   exactly once.
3. A forced outbox write failure returns 503 for that request and every later submit or cancel, and
   `/actuator/health` reports `DOWN`.
4. A malformed trade message is on the DLT within one second, with exception headers, and settlement keeps
   consuming later records.
5. A transient settlement failure is retried and succeeds without dead-lettering.
6. A dead-lettered valid trade can be listed and replayed via the ops API and is booked exactly once.
7. `./mvnw verify` is green locally (with Docker) and in CI.

## 9. Alternatives considered

- **Batched commits on a separate thread:** higher throughput, but adds backpressure and ordered-acknowledgement
  complexity. A natural phase-3 optimisation once JMH and Gatling show the per-order commit cost.
- **Debezium CDC:** the industry-standard outbox relay, but adds a Kafka Connect container (about 1 GB of RAM on an
  8 GB dev machine) and moves the interesting logic out of Java code.
- **`SELECT … FOR UPDATE SKIP LOCKED` without an advisory lock:** allows concurrent relays, but they can publish rows
  out of order, which breaks per-symbol ordering.
- **Postgres `failed_trades` table instead of reading the DLT:** easier querying and replay tracking, but a second
  source of truth. Declined in favour of the DLT alone.
