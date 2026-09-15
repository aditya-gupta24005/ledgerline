# Ledgerline Phase 2 Design

Date: 2026-09-15
Status: approved in chat, pending written-spec review

Phase 2 is four sub-projects, built and committed in this order: **A → C → B → D**.

| | Sub-project | Outcome |
|---|---|---|
| A | Guaranteed trade publishing | A trade is never acknowledged unless it is durable; Kafka outages lose nothing; poison messages are quarantined |
| C | Risk & P&L service | Real-time positions, average cost, realised/unrealised P&L and limit alerts per account and symbol |
| B | Security | Keycloak-issued JWTs on every service; TRADER / RISK / OPS roles; traders act only on their own account |
| D | Live dashboard | React + TypeScript app with live depth, trade tape, order ticket and positions |

Everything in phase 1 keeps working, and all existing tests must keep passing after each sub-project.

---

## A. Guaranteed trade publishing

### Problem

`KafkaTradeEventPublisher` sends trades asynchronously and only logs failures. If Kafka is down, or the
process dies before a send completes, the client has already been told the order filled, yet settlement
never hears about the trade.

### Design: transactional outbox with fail-stop

**Storage.** order-service gets a datasource pointing at the existing `ledgerline` Postgres database, with
its own schema `orders` managed by Flyway (`spring.flyway.schemas=orders`). Settlement keeps `public`. One
database instance, separate schemas, and no service reads another's tables.

```sql
CREATE TABLE orders.outbox_events (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id      VARCHAR(64)  NOT NULL UNIQUE,   -- trade id
    topic         VARCHAR(128) NOT NULL,
    message_key   VARCHAR(64)  NOT NULL,          -- symbol
    event_type    VARCHAR(64)  NOT NULL,          -- "TradeExecuted"
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL,
    published_at  TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON orders.outbox_events (id) WHERE published_at IS NULL;
```

**Write path.** Inside the sequencer task, after `engine.submit(...)` produces trades, `OutboxWriter`
inserts every trade of that command as one JDBC batch in one transaction (`JdbcClient`, no JPA on the hot
path). The HTTP response is built only after the commit. `TradeEventPublisher` and
`KafkaTradeEventPublisher` are removed.

**Fail-stop.** If the outbox write throws, the engine's in-memory state already contains trades that are
not durable. `OrderGateway` moves to `HALTED`:
- the failing request returns **503** (`EngineHaltedException` → ProblemDetail, title "Matching engine halted");
- every later submit and cancel returns 503 without touching the engine, while book reads still work;
- a `matchingEngine` `HealthIndicator` reports `DOWN` with the halt reason.

Recovery is a restart. The in-memory book is lost on restart, which is a known limitation until engine
journaling in phase 4. Halting beats continuing with trades nobody recorded, and it is how real venues
behave.

**Relay.** `OutboxRelay` runs `@Scheduled(fixedDelay = 200ms)` (property `ledgerline.outbox.poll-interval`):
1. In a transaction: `SELECT … WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`.
2. For each row in order: deserialize the payload into `TradeExecuted` and send it with
   `KafkaTemplate<String, TradeExecuted>`, waiting up to 5 s for the ack.
3. On the first failure, stop the batch so later rows never overtake an earlier one for the same symbol.
4. Mark the rows that were acknowledged with `published_at = now()`.

Delivery is at least once. A crash between the ack and the update resends the row, and settlement already
de-duplicates by trade id (risk-service also does, see C). Producer settings stay `acks=all`,
`enable.idempotence=true`.

**Housekeeping and visibility.**
- `OutboxCleanup` runs daily and deletes rows published more than 7 days ago (`ledgerline.outbox.retention`).
- A Micrometer gauge `ledgerline.outbox.pending` counts unpublished rows.

### Design: dead-letter topic in settlement

- `DefaultErrorHandler` with `ExponentialBackOffWithMaxRetries(3)`, starting at 500 ms and doubling.
- After retries are exhausted, `DeadLetterPublishingRecoverer` publishes the original record (bytes and
  headers, including the exception headers) to **`ledgerline.trades.executed.DLT`**, declared as a
  `NewTopic` with 3 partitions.
- Some failures go straight to the DLT without retrying: `DeserializationException`,
  `IllegalArgumentException` (invalid event), `MessageConversionException`.
- The recoverer uses a dedicated `KafkaTemplate<Object, Object>` whose value serializer handles both
  `byte[]` (raw bytes from failed deserialization) and `TradeExecuted` (a `DelegatingByTypeSerializer`).

### Tests (A)

- `OutboxWriterIT` (Testcontainers Postgres): a batch insert is atomic, and a duplicate `event_id` fails.
- `OrderGatewayTest`: an outbox failure halts the engine, and later submits are rejected without mutating the book.
- `OutboxRelayIT` (Testcontainers Postgres + Kafka):
  - rows are published in id order and marked published;
  - with the broker unreachable, rows stay pending and the batch stops at the first failure;
  - once the broker is back, everything is delivered.
- `OrderControllerTest`: a halted engine returns 503, and health is `DOWN`.
- `DeadLetterIT` in settlement (Testcontainers Kafka + Postgres): a malformed message lands on the DLT,
  and the next valid trade is still settled.

---

## C. Risk & P&L service

New module **`risk-service`** (port 8083), with Kafka Streams application id `risk-service` and
`processing.guarantee=exactly_once_v2`.

### Domain: `Position` (pure Java, unit tested)

Key `accountId|symbol`. Fields:
- `netQuantity` (long): positive means long, negative means short.
- `averageCost` (BigDecimal, scale 4, `HALF_EVEN`).
- `realizedPnl` (BigDecimal).
- `lastPrice` (BigDecimal, price of the latest fill).
- `notional` = |netQuantity| × lastPrice.
- `limitBreachedByLastFill` (boolean).

`apply(Fill fill)`, where `Fill` = signed quantity and price, uses average-cost accounting:
- **Opening or adding** in the same direction: `averageCost` becomes the quantity-weighted average.
- **Reducing:** `realizedPnl += (price − averageCost) × closedQty × sign(netQuantity)`; `averageCost` is unchanged.
- **Flipping** (for example long 10, sell 15): close the 10 as above, then open 5 short at `price`.
- **Flat:** `averageCost = 0`.
- `limitBreachedByLastFill` = previous notional ≤ limit and new notional > limit, with the limit from
  `ledgerline.risk.position-notional-limit` (default 1,000,000).

### Topology

```
trades (ledgerline.trades.executed, key=symbol)
  └─ dedupe by tradeId ─ processor with a 7-day WindowStore "seen-trades"
       ├─ flatMap → 2 fills keyed accountId|symbol (buyer +qty, seller −qty)
       │     └─ groupByKey → aggregate(Position::apply) → KTable store "positions"
       │           └─ toStream.filter(limitBreachedByLastFill) → RiskAlert → ledgerline.risk.alerts
       └─ mapValues(price) → groupByKey → reduce(latest) → store "last-prices" (key=symbol)
```

- **Why the dedupe step exists:** `exactly_once_v2` guarantees Kafka Streams doesn't double-apply a
  record it reads once. It cannot detect the same trade published twice by the outbox relay, so a
  duplicate would otherwise double-count a position.
- **Serdes:** JSON serdes from Spring Kafka for `TradeExecuted`, `Position` and `RiskAlert`.

### API

`GET /api/v1/risk/accounts/{accountId}/positions` returns, for each symbol:
- net quantity, average cost, last price and notional;
- realised P&L, and unrealised P&L = (last price − average cost) × netQuantity.

Positions come from a `prefixScan("accountId|")` on the `positions` store, and last prices from
`last-prices`. The endpoint returns 503 ProblemDetail until Kafka Streams is `RUNNING`.

### Known limits (documented)

- **Single instance:** interactive queries across instances aren't implemented.
- **Post-trade only:** no pre-trade limit check; orders aren't blocked.
- **Trade prices only:** no external market-data feed.

### Tests (C)

- `PositionTest`: open, add, reduce, flip to short, close flat, realised P&L sign for longs and shorts, and limit crossing only once.
- `RiskTopologyTest` (`TopologyTestDriver`, no Docker):
  - one trade produces correct positions for both accounts;
  - a duplicate trade id is ignored;
  - an alert is emitted once when crossing the limit;
  - last price updates.
- `PositionControllerTest`: response mapping and unrealised P&L, plus 503 when streams aren't ready.

---

## B. Security with Keycloak

### Identity provider

- **Container:** Keycloak `26.7.3` in `start-dev --import-realm` mode, mounting
  `keycloak/ledgerline-realm.json`, with the admin console at `:8180`.
- **Realm:** `ledgerline`.
- **Realm roles:** `TRADER`, `RISK`, `OPS`.
- **Dev users** (password = username; documented as dev-only):

  | User | Role |
  |---|---|
  | `alice`, `bob`, `carol` | TRADER |
  | `rita` | RISK |
  | `oscar` | OPS |

- **Clients:**
  - `ledgerline-dashboard`: public client, authorization code flow with PKCE S256, redirect
    `http://localhost:5173/*`, web origin `http://localhost:5173`.
  - `ledgerline-cli`: public client with direct access grants, for the curl demo only and labelled dev-only.
  - Both use an audience mapper that adds `ledgerline-api` to access tokens.

### Shared module `ledgerline-security`

A Spring Boot auto-configuration used by order-, settlement- and risk-service:
- **Resource server:** JWT validation via `issuer-uri` (`http://localhost:8180/realms/ledgerline`),
  plus an **audience validator** requiring `ledgerline-api`.
- **Roles:** a converter mapping `realm_access.roles` to `ROLE_TRADER`, `ROLE_RISK` and `ROLE_OPS`.
- **`CurrentUser`:** a helper exposing the account id (`preferred_username`) and roles.
- **CORS:** allowed origins from `ledgerline.security.cors.allowed-origins` (default `http://localhost:5173`).
- **Stateless:** CSRF disabled for this bearer-token API.
- **Public:** `/actuator/health` and `/actuator/info` need no token.

### Authorization rules

| Service | Endpoint | Rule |
|---|---|---|
| order | `POST /api/v1/orders` | TRADER. The account is taken from the token, and `accountId` is **removed** from the request body |
| order | `DELETE /api/v1/orders/{symbol}/{id}` | TRADER. The engine only cancels an order owned by that account; otherwise 404, so nobody learns other orders exist |
| order | `GET /api/v1/books/{symbol}` | any authenticated user |
| order | WebSocket `/ws` (see D) | valid token on STOMP CONNECT |
| settlement | `GET /api/v1/accounts/{id}/balances` | RISK or OPS for any account; TRADER only when `{id}` matches the token |
| risk | `GET /api/v1/risk/accounts/{id}/positions` | RISK or OPS for any account; TRADER only when `{id}` matches the token |

**Engine API change:** `MatchingEngine.cancel(symbol, orderId, accountId)` returns false when the resting
order belongs to another account.

**Timing:** sub-projects A and C keep today's unauthenticated API, with `accountId` still in the order
body. The body field is removed, and ownership enforced, only as part of B, in the same commits that turn
security on, so the API is never half-secured.

**Artifacts:** `spring-boot-starter-security-oauth2-resource-server` (Boot 4 name) and
`spring-boot-starter-security-test`. Kafka Streams serdes use Spring Kafka's `JacksonJsonSerde`.

### Tests (B)

- MockMvc with `spring-security-test` `jwt()` in each service:
  - no token → 401;
  - wrong role → 403;
  - a trader reading another account → 403;
  - a trader cancelling someone else's order → 404;
  - RISK and OPS can read any account.
- Unit tests for the role converter and the audience validator (missing or wrong audience rejected).
- Engine test for owner-checked cancel.

---

## D. Live dashboard

### Server side (order-service)

- **Endpoint:** `spring-boot-starter-websocket`, STOMP endpoint `/ws`, simple broker on `/topic`.
- **Topics:**
  - `/topic/books.{symbol}`: top-10 depth snapshot (same shape as `BookResponse`);
  - `/topic/trades.{symbol}`: each trade, sent after its outbox commit.
- **Conflation:** each command marks its symbol dirty. A 100 ms scheduled task takes one snapshot per
  dirty symbol through the gateway and publishes it, so there are at most 10 updates a second per symbol
  however fast orders arrive.
- **Auth:** a `ChannelInterceptor` on `CONNECT` reads the `Authorization: Bearer` native header, decodes
  it with the shared `JwtDecoder`, and sets the principal. Anything else is rejected.

### Client (`dashboard/`, React + TypeScript + Vite)

- **Libraries:** `oidc-client-ts` for login (authorization code + PKCE against `ledgerline-dashboard`)
  and `@stomp/stompjs` for WebSocket. Plain CSS, no global state library.
- **Components:**
  - `AuthGate`: login and logout, shows the user and role.
  - `SymbolPicker`: ACME, GLOBX, INITECH.
  - `DepthLadder`: bids and asks with quantity bars.
  - `TradeTape`: the last 50 trades.
  - `OrderTicket`: limit or market, buy or sell; shows the API response or problem detail.
  - `PositionsPanel`: polls risk-service every 2 s.
- **Vite dev server** on port 5173; API base URLs come from `.env`.
- **Tests:** Vitest for the pure logic (depth-ladder scaling, tape trimming, P&L formatting).

### CI

A second GitHub Actions job builds the dashboard: Node 22, `npm ci`, `npm test`, `npm run build`.

---

## Cross-cutting

- **Commits:** small commits per task, never one large dump.
- **README:** architecture diagram, security matrix and a runnable `scripts/demo.sh` that gets tokens
  and walks through trading, balances, positions and alerts.
- **Docker Compose:** gains the Keycloak realm import.
- **Memory:** the 8 GB machine must run Kafka, Postgres, Keycloak and four JVMs. Services get
  `-Xmx384m` in the demo script.
- **Resume claims:** only measured or tested claims.

## Out of scope for phase 2

- Pre-trade risk checks.
- Multi-instance Kafka Streams queries.
- Engine journaling and replay.
- A market-data feed and exchange holiday calendars.
- TLS and secret management.
- Kubernetes, Terraform and FIX (phases 3–4).
