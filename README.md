# Ledgerline

An event-driven trading and post-trade platform in Java: order entry, a price-time priority
matching engine, and a double-entry settlement ledger, connected by Kafka.

```
Client ──REST──► order-service ──────────────► Kafka: ledgerline.trades.executed ──► settlement-service ──► PostgreSQL
                  │  single-threaded sequencer        (keyed by symbol)                 │  idempotent booking
                  └─► matching-engine (in-memory)                                         └─ double-entry journal, T+1
```

## Modules

| Module | What it does | Tech |
|---|---|---|
| `matching-engine` | Limit order book with price-time priority, limit and market orders, partial fills, cancels | Plain Java 21, JUnit 5, AssertJ |
| `order-service` | REST order entry, validation, sequencing into the engine, trade publishing | Spring Boot 4, Bean Validation, Spring Kafka |
| `settlement-service` | Consumes trades, books balanced journal entries, schedules T+1 settlement, serves balances | Spring Boot 4, Spring Data JPA, Flyway, PostgreSQL |
| `ledgerline-events` | Event contracts shared over Kafka | Java records |

## Design decisions

- **Single-writer matching.** The engine is intentionally not thread-safe. `OrderGateway` funnels every
  command through one dedicated thread (the LMAX approach), so matching needs no locks, is deterministic,
  and trades are published in match order.
- **Integer price ticks.** Prices cross the API as `BigDecimal` and are converted to `long` ticks of 0.0001.
  The matching loop does integer comparisons only, and a price finer than one tick is rejected rather than
  silently rounded.
- **O(1) cancels.** Orders at a price level form an intrusive doubly linked list, and an id index points at
  each resting order, so cancelling doesn't scan the queue.
- **Per-symbol ordering.** Trades are keyed by symbol, so all fills for an instrument land on one partition
  and are consumed in order.
- **Exactly-once effect over at-least-once delivery.** The trade id is the idempotency key. A redelivered
  trade is skipped, and a concurrent duplicate hits the primary key and rolls back.
- **Double-entry invariant.** For every asset, a trade's postings sum to zero (cash moves buyer to seller,
  securities move seller to buyer). This is checked before anything is written. Balances are always
  derived from the append-only journal, never stored.
- **Money is never `double`.** `BigDecimal` at the edges, `NUMERIC` in the database.

## Running locally

Prerequisites: JDK 21+ and Docker.

```bash
docker compose up -d                                   # Kafka, kafka-ui, PostgreSQL, Keycloak
./mvnw install                                         # build and test everything
./mvnw -pl order-service spring-boot:run               # http://localhost:8081
./mvnw -pl settlement-service spring-boot:run          # http://localhost:8082
```

Kafka UI is at http://localhost:8090.

### Try it

```bash
# Alice offers 100 ACME at 101.50
curl -s -X POST localhost:8081/api/v1/orders -H 'Content-Type: application/json' \
  -d '{"accountId":"alice","symbol":"ACME","side":"SELL","type":"LIMIT","price":101.50,"quantity":100}'

# Bob buys 40 at up to 102: fills at 101.50
curl -s -X POST localhost:8081/api/v1/orders -H 'Content-Type: application/json' \
  -d '{"accountId":"bob","symbol":"ACME","side":"BUY","type":"LIMIT","price":102,"quantity":40}'

curl -s localhost:8081/api/v1/books/ACME                 # 60 left on the ask
curl -s localhost:8082/api/v1/accounts/bob/balances      # ACME +40, USD -4060
```

## Testing

- **Matching engine:** scenario tests for priority, partial fills, market orders and cancels, plus a seeded
  random order-flow test. It checks after every operation that the book never crosses, levels stay sorted,
  and resting quantity matches an independent model.
- **Order service:** full HTTP-to-engine tests with MockMvc; only the Kafka publisher is mocked.
- **Settlement:** unit tests for postings and business-day math, plus a Testcontainers integration test
  against real PostgreSQL. It is skipped automatically when Docker isn't available, and CI always runs it.

## Roadmap

- [x] **Phase 1:** matching engine, order service, Kafka, settlement ledger, Docker Compose, CI
- [ ] **Phase 2:** OAuth2 with Keycloak and roles (trader, risk, ops); real-time risk and P&L with Kafka
  Streams; WebSocket market-depth dashboard; transactional outbox for guaranteed trade publishing;
  dead-letter topic
- [ ] **Phase 3:** JMH benchmarks for the matching engine; Gatling load tests; Micrometer, Prometheus,
  Grafana and OpenTelemetry
- [ ] **Phase 4:** Kubernetes (Helm) on GKE with Terraform; FIX order entry via QuickFIX/J; engine journaling
  and replay; exchange holiday calendars
