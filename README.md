# Ledgerline

An event-driven trading and post-trade platform in Java: order entry, a price-time priority
matching engine, and a double-entry settlement ledger, connected by Kafka.

```
Client ──REST──► order-service ─┬─► matching-engine (in-memory, single-threaded sequencer)
                                └─► outbox_events (Postgres, same tx boundary as the reply)
                                         │  relay: advisory lock, id order, at-least-once
                                         ▼
                        Kafka: ledgerline.trades.executed (keyed by symbol)
                                         │
                                         ▼
                                 settlement-service ──► PostgreSQL: double-entry journal, T+1
                                         │ retry 1s → 2s → 4s, then
                                         ▼
                        Kafka: ledgerline.trades.executed.DLT ◄── ops API: list and replay
```

```
Kafka: ledgerline.trades.executed ──► risk-service (Kafka Streams, exactly_once_v2)
                                        dedupe by trade id → fills → positions (avg cost, realised P&L)
                                        ├─► last traded price per symbol
                                        ├─► Kafka: ledgerline.risk.alerts (first crossing of the notional limit)
                                        └─► REST: /api/v1/risk/accounts/{id}/positions (marked to market)
```

## Modules

| Module | What it does | Tech |
|---|---|---|
| `matching-engine` | Limit order book with price-time priority, limit and market orders, partial fills, cancels | Plain Java 21, JUnit 5, AssertJ |
| `order-service` | REST order entry, validation, sequencing into the engine, transactional outbox and relay to Kafka | Spring Boot 4, Bean Validation, JDBC, Flyway, Spring Kafka |
| `settlement-service` | Consumes trades, books balanced journal entries, schedules T+1 settlement, serves balances; retries, dead-letter topic and replay API | Spring Boot 4, Spring Data JPA, Flyway, PostgreSQL, Spring Kafka |
| `risk-service` | Real-time positions, average-cost realised/unrealised P&L, limit alerts, positions API | Spring Boot 4, Kafka Streams, RocksDB state stores |
| `ledgerline-security` | Shared JWT resource-server auto-configuration: Keycloak role mapping, username principal, method security, CORS | Spring Security 7 |
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
- **Transactional outbox.** A command's trades are written to Postgres before the client gets its response, and a
  relay publishes them afterwards. A Kafka outage delays trades but never loses them.
- **Fail-stop on persistence failure.** If the outbox write fails, the book already holds trades that aren't durable,
  so order entry halts (503, health `DOWN`) until restart, as a real venue would.
- **One relay at a time.** A Postgres advisory lock allows one publisher, so trades for a symbol can't be reordered
  (`SKIP LOCKED` would allow concurrent relays to interleave).
- **Exactly-once effect over at-least-once delivery.** The trade id is the idempotency key. Redeliveries and replays
  are skipped, and a concurrent duplicate hits the primary key and rolls back.
- **Poison messages don't block.** Unparseable or invalid trades go straight to a dead-letter topic; transient
  failures get three retries first. Ops can list dead letters and replay one.
- **Exactly-once processing isn't exactly-once input.** Kafka Streams runs with `exactly_once_v2`, but the outbox
  relay delivers at least once, so the risk topology also de-duplicates by trade id (7-day retention).
- **No state-store caching where alerts depend on it.** With caching, two fills in one commit interval collapse
  into one update and a limit crossing could be missed, so the positions store writes every change through.
- **Average-cost P&L.** Adding to a position re-weights the average cost, reducing it realises P&L against that
  cost, and crossing through zero closes the position and opens the remainder at the fill price.
- **Known limits of the risk service.**
  - Single instance: interactive queries only read local stores.
  - Post-trade only: nothing blocks an order before it trades.
  - Prices come from trades only: there is no market-data feed.
- **The account is the token, not the request.** `POST /orders` has no `accountId`; the engine trades under the
  JWT's `preferred_username`. A trader therefore cannot act for anyone else, and cancelling someone else's order
  returns 404, the same as an unknown order, so nothing leaks.
- **Rules next to the endpoint.** Each controller states its rule with `@PreAuthorize`; the shared module only
  decides what a valid token is (issuer, audience, signature, role mapping) and that everything but health needs one.
- **Double-entry invariant.** For every asset, a trade's postings sum to zero (cash moves buyer to seller,
  securities move seller to buyer). This is checked before anything is written. Balances are always
  derived from the append-only journal, never stored.
- **Money is never `double`.** `BigDecimal` at the edges, `NUMERIC` in the database.

## Running locally

Prerequisites: JDK 21+ and Docker.

```bash
docker compose up -d                                   # Kafka, kafka-ui, PostgreSQL, Keycloak (imports the realm)
./mvnw install                                         # build and test everything
./mvnw -pl order-service spring-boot:run               # http://localhost:8081
./mvnw -pl settlement-service spring-boot:run          # http://localhost:8082
./mvnw -pl risk-service spring-boot:run                # http://localhost:8083
```

Kafka UI is at http://localhost:8090.

Keycloak admin console: http://localhost:8180 (admin / admin). Realm `ledgerline`, dev users (password = username):

| User | Role | Can |
|---|---|---|
| alice, bob, carol | TRADER | place and cancel own orders; read own balances and positions; read the book |
| rita | RISK | read any account's balances and positions; read the book |
| oscar | OPS | everything RISK can, plus list and replay dead letters |

Every API call needs a bearer token. For curl, use the dev-only password grant:

```bash
source scripts/token.sh
curl -s localhost:8082/api/v1/accounts/bob/balances -H "$(auth bob)"
```

### Try it

```bash
source scripts/token.sh
# Alice offers 100 ACME at 101.50
curl -s -X POST localhost:8081/api/v1/orders -H "$(auth alice)" -H 'Content-Type: application/json' \
  -d '{"symbol":"ACME","side":"SELL","type":"LIMIT","price":101.50,"quantity":100}'

# Bob buys 40 at up to 102: fills at 101.50
curl -s -X POST localhost:8081/api/v1/orders -H "$(auth bob)" -H 'Content-Type: application/json' \
  -d '{"symbol":"ACME","side":"BUY","type":"LIMIT","price":102,"quantity":40}'

curl -s localhost:8081/api/v1/books/ACME -H "$(auth bob)"                 # 60 left on the ask
curl -s localhost:8082/api/v1/accounts/bob/balances -H "$(auth bob)"      # ACME +40, USD -4060
```

### Failure demos

```bash
DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker ./scripts/demo-kafka-outage.sh     # orders succeed while Kafka is down; trades delivered after
DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker ./scripts/demo-poison-message.sh   # bad message dead-lettered; valid dead letter replayed
DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker ./scripts/demo-risk.sh             # positions, P&L and a limit alert
./scripts/demo-security.sh                                                                        # 401/403/404 matrix across roles and accounts
```

Outbox backlog: `curl localhost:8081/actuator/metrics/ledgerline.outbox.pending -H "$(auth oscar)"`

## Testing

- **Matching engine:** scenario tests for priority, partial fills, market orders and cancels, plus a seeded
  random order-flow test. It checks after every operation that the book never crosses, levels stay sorted,
  and resting quantity matches an independent model.
- **Order service:** full HTTP-to-engine tests with MockMvc against real Postgres; only the outbox writer is mocked.
- **Outbox and dead letters:** Testcontainers tests cover atomic batch writes, halt on write failure, in-order
  publishing, resuming after a failed send, single relay under the advisory lock, dead-lettering without retries,
  retrying transient failures, and replay booking exactly once.
- **Database outage:** a warm connection pool, a graceful Postgres shutdown and an idle connection reproduce a real
  outage. Order entry must halt with a 503 within the engine's 5 s timeout, while book reads keep working.
  Hikari's default 30 s connection wait failed this, so the wait is capped at 2 s.
- **Risk:** unit tests for average-cost maths (adding, reducing, flipping, closing, limit crossing); `TopologyTestDriver`
  tests for de-duplication, retention, alerts, last prices and bad input; and an end-to-end test against real
  Kafka with `exactly_once_v2`.
- **Security:** MockMvc tests with fake JWTs cover 401 without a token, 403 for the wrong role or another
  trader's account, owner-only cancel, and public health; `KeycloakSecurityIT` starts a real Keycloak, gets
  real tokens, and proves issuer, signature, audience and role mapping end to end.
- **Settlement:** unit tests for postings and business-day math, plus a Testcontainers integration test
  against real PostgreSQL. It is skipped automatically when Docker isn't available, and CI always runs it.

## Roadmap

- [x] **Phase 1:** matching engine, order service, Kafka, settlement ledger, Docker Compose, CI
- [ ] **Phase 2:**
  - [x] A. Transactional outbox, fail-stop engine, dead-letter topic and replay API
  - [x] C. Real-time risk and P&L with Kafka Streams
  - [x] B. OAuth2 with Keycloak and roles (trader, risk, ops)
  - [ ] D. Live React dashboard over WebSocket
- [ ] **Phase 3:** JMH benchmarks for the matching engine; Gatling load tests; Micrometer, Prometheus,
  Grafana and OpenTelemetry
- [ ] **Phase 4:** Kubernetes (Helm) on GKE with Terraform; FIX order entry via QuickFIX/J; engine journaling
  and replay; exchange holiday calendars
