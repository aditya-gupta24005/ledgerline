# Ledgerline Phase 2 Design

Date: 2026-09-15
Status: approved in chat, pending written-spec review. Sub-project A is specified in detail in `2026-09-15-outbox-and-dead-letter-design.md`.

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

**Authoritative spec: [`2026-09-15-outbox-and-dead-letter-design.md`](2026-09-15-outbox-and-dead-letter-design.md)**
(approved earlier the same day). That document wins wherever this summary is less specific. In short:

- **Outbox:** order-service writes each command's trades to `order_service.outbox_events` in one transaction on
  the engine thread, before replying. `TradeEventPublisher` and `KafkaTradeEventPublisher` are removed.
- **Fail-stop:** if the outbox write fails, the engine halts. Submit and cancel return 503, book reads still
  work, health is `DOWN`, and a restart clears it.
- **Relay:** runs every 200 ms and holds a Postgres **advisory lock**, so exactly one relay publishes at a
  time. It sends rows in id order as plain JSON strings with an `eventType` header, stops at the first failure
  (recording `attempts` and `last_error`), and marks acknowledged rows as published. Delivery is at least once.
  `FOR UPDATE SKIP LOCKED` was rejected because concurrent relays could reorder a symbol's trades.
- **Settlement:** consumes plain JSON strings, so no Java class names cross the wire. Retries go 1 s, 2 s,
  4 s. Invalid messages and unbalanced postings go straight to `ledgerline.trades.executed.DLT`.
- **Ops API:** `GET /api/v1/ops/dead-letters` lists dead-lettered records;
  `POST /api/v1/ops/dead-letters/{partition}/{offset}/replay` republishes one. Replay is safe to repeat
  because settlement is idempotent.
- **Metrics:** `ledgerline.outbox.pending`, `ledgerline.outbox.published`, `ledgerline.outbox.publish.failures`.
- **Deferred:** outbox row cleanup and securing the ops API (added in B under the OPS role).

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
- **Serdes:** trades arrive as plain JSON strings (see A), so the input serde is `JacksonJsonSerde<TradeExecuted>` with a fixed target type, ignoring type headers. `Position` and `RiskAlert` also use `JacksonJsonSerde`.

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
| settlement | `GET` and `POST /api/v1/ops/dead-letters/**` | OPS |
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
