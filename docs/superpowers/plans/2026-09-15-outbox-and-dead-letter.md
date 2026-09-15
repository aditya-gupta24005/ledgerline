# Outbox and Dead-Letter Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A trade is durable before the client hears about it, reaches Kafka even if Kafka was down when it
matched, and a settlement message that cannot be processed goes to a dead-letter topic where ops can inspect
and replay it.

**Architecture:**
- **order-service:** writes each command's trades to a Postgres outbox table on the engine thread, and halts
  order entry if that write fails. An advisory-locked scheduled relay publishes rows to Kafka in id order as
  plain JSON strings.
- **settlement-service:** consumes plain JSON, retries transient failures with exponential backoff, sends
  permanent failures to `ledgerline.trades.executed.DLT`, and exposes an ops API to list and replay
  dead-lettered records.

**Tech Stack:** Java 21, Spring Boot 4.1.1 (JDBC, Flyway, Spring Kafka 4.1), Jackson 3 (`tools.jackson`),
PostgreSQL 17, Apache Kafka 4.2.1, JUnit 5, Mockito, Awaitility, Testcontainers 2.0.5.

**Spec:** `docs/superpowers/specs/2026-09-15-outbox-and-dead-letter-design.md` (authoritative for this
sub-project; overview in `docs/superpowers/specs/2026-09-15-phase-2-design.md`).

## Global Constraints

- **Repo root:** `/Users/adityagupta/Developer/ledgerline`. Run Maven from there with `./mvnw`.
- **Versions:** no dependency versions outside the Spring Boot 4.1.1 BOM, except modules already managed in
  the root `pom.xml`.
- **Packages:** base `dev.ledgerline.order` (order-service) and `dev.ledgerline.settlement` (settlement-service).
- **Schema:** order-service uses schema **`order_service`**. Every runtime SQL statement uses the qualified
  name `order_service.outbox_events`.
- **Topics:** `ledgerline.trades.executed`, and `ledgerline.trades.executed.DLT` (3 partitions, replication
  factor 1).
- **Kafka headers:** `eventType` = `TradeExecuted` on outbox messages; `ledgerline-replayed-from` =
  `ledgerline.trades.executed.DLT/<partition>/<offset>` on replayed messages.
- **Advisory lock key:** `7401001`.
- **Relay properties:** `ledgerline.outbox.relay.poll-interval` (default `200ms`), `ledgerline.outbox.relay.batch-size`
  (default `100`), `ledgerline.outbox.relay.send-timeout` (default `5s`).
- **Metric names:** `ledgerline.outbox.pending` (gauge), `ledgerline.outbox.published` (counter),
  `ledgerline.outbox.publish.failures` (counter).
- **Halt response:** 503 ProblemDetail, title `Engine halted`, detail
  `Order entry halted after a persistence failure; restart required`.
- **Settlement retries:** exponential backoff, initial 1 s, multiplier 2, max 3 retries. The initial interval
  is configurable via `ledgerline.settlement.retry.initial-interval`, for tests only.
- **Scheduling toggle:** `ledgerline.scheduling.enabled` (default `true`) is the only switch for scheduling.
  Tests set it to `false` and call `OutboxRelay.runOnce()` directly. This is a testability addition to spec §4.4.
- **Deliberate deviations from the spec:**
  - Runtime SQL is schema-qualified (`order_service.outbox_events`) instead of relying on `currentSchema` in the
    JDBC URL, because Testcontainers' `@ServiceConnection` replaces the URL.
  - The order-service producer keeps `max.block.ms: 5000` (spec §6 said to remove it), so a relay run fails fast
    while the broker is down instead of blocking for the 60 s default.
  - `DeadLetterIT` asserts dead-lettering within 5 s rather than 1 s to stay stable in CI. Retries would take at
    least 7 s, so the test still proves none happened.
- **Docker-backed tests:** `@Testcontainers(disabledWithoutDocker = true)`, with images `postgres:17-alpine`
  and `apache/kafka:4.2.1`.
- **Docker CLI:** `/Applications/Docker.app/Contents/Resources/bin/docker` (not on PATH). Testcontainers
  finds Docker Desktop on its own.
- **Commits:** one per task, no attribution lines. Run `./mvnw -B verify` green before committing.

## File Structure

**order-service**

| File | Responsibility |
|---|---|
| `pom.xml` | Adds JDBC, Flyway, Postgres, Testcontainers (Postgres, Kafka), Awaitility, failsafe |
| `src/main/resources/application.yml` | Datasource, Flyway schema, String Kafka serializers, relay properties |
| `src/main/resources/db/migration/V1__outbox.sql` | `outbox_events` table and unpublished index |
| `…/order/outbox/OutboxWriter.java` | Interface: `void append(List<TradeExecuted>)` |
| `…/order/outbox/JdbcOutboxWriter.java` | One-transaction JDBC batch insert |
| `…/order/outbox/OutboxRelay.java` | Advisory-locked publish loop, metrics |
| `…/order/outbox/OutboxRelayProperties.java` | Typed relay properties |
| `…/order/SchedulingConfiguration.java` | `@EnableScheduling` guarded by `ledgerline.scheduling.enabled` |
| `…/order/engine/OrderGateway.java` | Uses `OutboxWriter`; halt state |
| `…/order/engine/EngineHaltedException.java` | Thrown while halted |
| `…/order/engine/EngineHealthIndicator.java` | `DOWN` with reason and time when halted |
| `…/order/api/ApiExceptionHandler.java` | Maps `EngineHaltedException` to 503 |
| `…/order/events/*` | **Deleted** (`TradeEventPublisher`, `KafkaTradeEventPublisher`) |
| `src/test/java/…/order/PostgresTestConfiguration.java`, `KafkaTestConfiguration.java` | Shared container beans |
| `src/test/java/…/order/outbox/OutboxWriterIT.java`, `OutboxRelayIT.java`, `OutboxRelayFailureIT.java`, `OutboxRelayLockIT.java` | Outbox tests |
| `src/test/java/…/order/engine/OrderGatewayTest.java` | Halt behaviour (unit) |
| `src/test/java/…/order/api/OrderControllerTest.java`, `EngineHaltedApiTest.java` | API tests |

**ledgerline-events**

| File | Responsibility |
|---|---|
| `…/events/Topics.java` | Adds `TRADES_EXECUTED_DLT` |

**settlement-service**

| File | Responsibility |
|---|---|
| `pom.xml` | Adds Testcontainers Kafka, Awaitility, spring-kafka-test, webmvc-test |
| `src/main/resources/application.yml` | String deserializer and serializer, retry property |
| `…/settlement/messaging/TradeListener.java` | Parses JSON; wraps failures |
| `…/settlement/messaging/InvalidTradeMessageException.java` | Permanent-failure marker |
| `…/settlement/messaging/KafkaConfiguration.java` | Topics, `DefaultErrorHandler`, DLT recoverer |
| `…/settlement/ops/DeadLetterRecord.java`, `ReplayResult.java` | API DTOs |
| `…/settlement/ops/DeadLetterStore.java` | Reads DLT records (latest N, or one by partition and offset) |
| `…/settlement/ops/DeadLetterReplayer.java` | Republishes one record |
| `…/settlement/ops/UnknownDeadLetterPartitionException.java` | 400 marker |
| `…/settlement/ops/DeadLetterController.java` | `GET` list, `POST` replay |
| `…/settlement/SettlementServiceApplication.java` | Old `kafkaErrorHandler` bean removed |
| `src/test/java/…/settlement/PostgresTestConfiguration.java`, `KafkaTestConfiguration.java` | Shared container beans |
| `src/test/java/…/settlement/messaging/TradeListenerTest.java`, `DeadLetterIT.java`, `TransientRetryIT.java` | Messaging tests |
| `src/test/java/…/settlement/ops/DeadLetterOpsIT.java` | Ops API tests |

**Repo root**

| File | Responsibility |
|---|---|
| `README.md` | Updated architecture, design decisions, demos |
| `scripts/demo-kafka-outage.sh`, `scripts/demo-poison-message.sh` | Runnable demos |

---

### Task 1: Outbox table and writer

**Files:**
- Modify: `order-service/pom.xml`
- Modify: `order-service/src/main/resources/application.yml`
- Create: `order-service/src/main/resources/db/migration/V1__outbox.sql`
- Create: `order-service/src/main/java/dev/ledgerline/order/outbox/OutboxWriter.java`
- Create: `order-service/src/main/java/dev/ledgerline/order/outbox/JdbcOutboxWriter.java`
- Create: `order-service/src/test/java/dev/ledgerline/order/PostgresTestConfiguration.java`
- Create: `order-service/src/test/java/dev/ledgerline/order/KafkaTestConfiguration.java`
- Create: `order-service/src/test/java/dev/ledgerline/order/outbox/OutboxWriterIT.java`
- Modify: `order-service/src/test/java/dev/ledgerline/order/api/OrderControllerTest.java` (class annotations only)

**Interfaces:**
- Consumes: `dev.ledgerline.events.TradeExecuted`, `dev.ledgerline.events.Topics.TRADES_EXECUTED`,
  the Boot `JsonMapper` bean (`tools.jackson.databind.json.JsonMapper`), `java.time.Clock` bean
  (`OrderServiceApplication.clock()`).
- Produces:
  - `public interface OutboxWriter { void append(List<TradeExecuted> trades); }`
  - `JdbcOutboxWriter.EVENT_TYPE = "TradeExecuted"`
  - `PostgresTestConfiguration` (bean `PostgreSQLContainer postgres()`) and `KafkaTestConfiguration`
    (bean `KafkaContainer kafka()`), both annotated `@ServiceConnection`.

- [ ] **Step 1: Add dependencies to `order-service/pom.xml`**

Inside `<dependencies>`, after the actuator dependency, add:

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-flyway</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
```

After the existing `spring-boot-starter-webmvc-test` test dependency, add:

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
```

In `<build><plugins>`, after `spring-boot-maven-plugin`, add:

```xml
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
            </plugin>
```

- [ ] **Step 2: Add the migration**

Create `order-service/src/main/resources/db/migration/V1__outbox.sql`:

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

- [ ] **Step 3: Configure the datasource and Flyway**

Replace `order-service/src/main/resources/application.yml` with:

```yaml
server:
  port: 8081

spring:
  application:
    name: order-service
  mvc:
    problemdetails:
      enabled: true
  datasource:
    url: jdbc:postgresql://localhost:5432/ledgerline
    username: ledgerline
    password: ledgerline
  flyway:
    schemas: order_service
    default-schema: order_service
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
      acks: all
      properties:
        enable.idempotence: true
        # Never let an unreachable broker block the matching thread for long.
        max.block.ms: 2000

ledgerline:
  currency: USD

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```

(The Kafka producer section changes again in Task 3.)

- [ ] **Step 4: Create the shared test container configurations**

Create `order-service/src/test/java/dev/ledgerline/order/PostgresTestConfiguration.java`:

```java
package dev.ledgerline.order;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;

@TestConfiguration(proxyBeanMethods = false)
public class PostgresTestConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:17-alpine");
    }
}
```

Create `order-service/src/test/java/dev/ledgerline/order/KafkaTestConfiguration.java`:

```java
package dev.ledgerline.order;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;

@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestConfiguration {

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer("apache/kafka:4.2.1");
    }
}
```

- [ ] **Step 5: Write the failing test**

Create `order-service/src/test/java/dev/ledgerline/order/outbox/OutboxWriterIT.java`:

```java
package dev.ledgerline.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.order.PostgresTestConfiguration;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class OutboxWriterIT {

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JsonMapper jsonMapper;

    @BeforeEach
    void clearOutbox() {
        jdbc.update("DELETE FROM order_service.outbox_events");
    }

    static TradeExecuted trade(String tradeId, String symbol) {
        return new TradeExecuted(tradeId, symbol, "USD", new BigDecimal("101.5000"), 10,
                1, "bob", 2, "alice", "BUY", Instant.parse("2026-09-15T10:00:00Z"));
    }

    @Test
    void appendsOneRowPerTradeInOrderWithRoutingColumns() {
        TradeExecuted first = trade("T-w-1", "ACME");
        TradeExecuted second = trade("T-w-2", "GLOBX");

        outboxWriter.append(List.of(first, second));

        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT topic, message_key, event_type, payload::text AS payload, published_at, attempts "
                        + "FROM order_service.outbox_events ORDER BY id");
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(row -> row.get("message_key")).containsExactly("ACME", "GLOBX");
        assertThat(rows).allSatisfy(row -> {
            assertThat(row.get("topic")).isEqualTo(Topics.TRADES_EXECUTED);
            assertThat(row.get("event_type")).isEqualTo(JdbcOutboxWriter.EVENT_TYPE);
            assertThat(row.get("published_at")).isNull();
            assertThat(row.get("attempts")).isEqualTo(0);
        });

        TradeExecuted roundTripped = jsonMapper.readValue((String) rows.get(0).get("payload"), TradeExecuted.class);
        assertThat(roundTripped)
                .usingRecursiveComparison()
                .withComparatorForType(BigDecimal::compareTo, BigDecimal.class)
                .isEqualTo(first);
    }

    @Test
    void failingBatchLeavesNoPartialRows() {
        TradeExecuted valid = trade("T-w-3", "ACME");
        TradeExecuted keyTooLong = trade("T-w-4", "SYMBOLLONGERTHAN16");

        assertThatThrownBy(() -> outboxWriter.append(List.of(valid, keyTooLong)))
                .isInstanceOf(DataAccessException.class);

        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.outbox_events", Long.class)).isZero();
    }

    @Test
    void emptyListWritesNothing() {
        outboxWriter.append(List.of());

        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.outbox_events", Long.class)).isZero();
    }
}
```

The test sets `ledgerline.scheduling.enabled=false` even though no scheduler exists yet, so it stays correct
after Task 3.

- [ ] **Step 6: Run the test to verify it fails**

Run: `./mvnw -B -pl ledgerline-events,matching-engine install -q && ./mvnw -B -pl order-service verify -Dit.test=OutboxWriterIT -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: test compilation fails with `cannot find symbol … class OutboxWriter`.

- [ ] **Step 7: Implement the writer**

Create `order-service/src/main/java/dev/ledgerline/order/outbox/OutboxWriter.java`:

```java
package dev.ledgerline.order.outbox;

import dev.ledgerline.events.TradeExecuted;
import java.util.List;

/**
 * Durably records trades for later publication. Implementations must write all trades of one call
 * atomically, or throw and write none.
 */
public interface OutboxWriter {

    void append(List<TradeExecuted> trades);
}
```

Create `order-service/src/main/java/dev/ledgerline/order/outbox/JdbcOutboxWriter.java`:

```java
package dev.ledgerline.order.outbox;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** Batch-inserts a command's trades into the outbox in a single transaction. */
@Component
class JdbcOutboxWriter implements OutboxWriter {

    static final String EVENT_TYPE = "TradeExecuted";

    private static final String INSERT = """
            INSERT INTO order_service.outbox_events (topic, message_key, event_type, payload, created_at)
            VALUES (?, ?, ?, ?::jsonb, ?)""";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    JdbcOutboxWriter(JdbcTemplate jdbc, TransactionTemplate transactions, JsonMapper jsonMapper, Clock clock) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    @Override
    public void append(List<TradeExecuted> trades) {
        if (trades.isEmpty()) {
            return;
        }
        Timestamp now = Timestamp.from(clock.instant());
        transactions.executeWithoutResult(status ->
                jdbc.batchUpdate(INSERT, trades, trades.size(), (statement, trade) -> {
                    statement.setString(1, Topics.TRADES_EXECUTED);
                    statement.setString(2, trade.symbol());
                    statement.setString(3, EVENT_TYPE);
                    statement.setString(4, jsonMapper.writeValueAsString(trade));
                    statement.setTimestamp(5, now);
                }));
    }
}
```

- [ ] **Step 8: Keep the existing controller test booting with a database**

In `order-service/src/test/java/dev/ledgerline/order/api/OrderControllerTest.java`, replace the class
annotations:

```java
@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
@AutoConfigureMockMvc
class OrderControllerTest {
```

with:

```java
@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@AutoConfigureMockMvc
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class OrderControllerTest {
```

and add these imports:

```java
import dev.ledgerline.order.PostgresTestConfiguration;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `./mvnw -B -pl order-service verify`
Expected: `OutboxWriterIT` passes 3 tests, `OrderControllerTest` passes 7 tests, `BUILD SUCCESS`.

- [ ] **Step 10: Commit**

```bash
git add order-service/pom.xml order-service/src/main/resources order-service/src/main/java/dev/ledgerline/order/outbox order-service/src/test/java/dev/ledgerline/order
git commit -m "Add transactional outbox table and JDBC writer to order-service"
```

### Task 2: Write trades to the outbox and halt on failure

**Files:**
- Create: `order-service/src/main/java/dev/ledgerline/order/engine/EngineHaltedException.java`
- Create: `order-service/src/main/java/dev/ledgerline/order/engine/EngineHealthIndicator.java`
- Modify (replace): `order-service/src/main/java/dev/ledgerline/order/engine/OrderGateway.java`
- Modify: `order-service/src/main/java/dev/ledgerline/order/api/ApiExceptionHandler.java`
- Delete: `order-service/src/main/java/dev/ledgerline/order/events/TradeEventPublisher.java`
- Delete: `order-service/src/main/java/dev/ledgerline/order/events/KafkaTradeEventPublisher.java`
- Create: `order-service/src/test/java/dev/ledgerline/order/engine/OrderGatewayTest.java`
- Create: `order-service/src/test/java/dev/ledgerline/order/api/EngineHaltedApiTest.java`
- Modify (replace): `order-service/src/test/java/dev/ledgerline/order/api/OrderControllerTest.java`

**Interfaces:**
- Consumes: `OutboxWriter.append(List<TradeExecuted>)` (Task 1), `PostgresTestConfiguration` (Task 1).
- Produces:
  - `OrderGateway(OutboxWriter outboxWriter, Clock clock, String currency)`
  - `OrderGateway.submit(OrderRequest): OrderOutcome`, `cancel(String, long): boolean`,
    `book(String, int): BookSnapshot`, `halt(): Optional<OrderGateway.Halt>`
  - `public record OrderGateway.Halt(String reason, Instant haltedAt)`
  - `OrderGateway.HALTED_DETAIL = "Order entry halted after a persistence failure; restart required"`
  - `EngineHaltedException(String message, Throwable cause)`
  - Health contributor named `matchingEngine`

- [ ] **Step 1: Write the failing unit test**

Create `order-service/src/test/java/dev/ledgerline/order/engine/OrderGatewayTest.java`:

```java
package dev.ledgerline.order.engine;

import static dev.ledgerline.matching.Side.BUY;
import static dev.ledgerline.matching.Side.SELL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.matching.BookSnapshot;
import dev.ledgerline.matching.OrderRequest;
import dev.ledgerline.order.outbox.OutboxWriter;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class OrderGatewayTest {

    private static final Instant NOW = Instant.parse("2026-09-15T10:00:00Z");
    private static final long PRICE_100 = Prices.toTicks(new BigDecimal("100"));

    private final RecordingOutboxWriter outbox = new RecordingOutboxWriter();
    private final OrderGateway gateway = new OrderGateway(outbox, Clock.fixed(NOW, ZoneOffset.UTC), "USD");

    @AfterEach
    void stopSequencer() throws InterruptedException {
        gateway.destroy();
    }

    @Test
    void ordersWithoutTradesDoNotTouchTheOutbox() {
        outbox.failNext = true;

        gateway.submit(OrderRequest.limit("alice", "ACME", SELL, PRICE_100, 5));

        assertThat(outbox.batches).isEmpty();
        assertThat(gateway.halt()).isEmpty();
    }

    @Test
    void allTradesOfOneCommandAreAppendedAsOneBatch() {
        gateway.submit(OrderRequest.limit("alice", "ACME", SELL, PRICE_100, 2));
        gateway.submit(OrderRequest.limit("carol", "ACME", SELL, PRICE_100, 3));

        OrderOutcome outcome = gateway.submit(OrderRequest.limit("bob", "ACME", BUY, PRICE_100, 5));

        assertThat(outbox.batches).singleElement().satisfies(batch -> {
            assertThat(batch).extracting(TradeExecuted::quantity).containsExactly(2L, 3L);
            assertThat(batch).isEqualTo(outcome.trades());
        });
    }

    @Test
    void outboxFailureHaltsOrderEntryButKeepsBookReadable() {
        gateway.submit(OrderRequest.limit("alice", "ACME", SELL, PRICE_100, 5));
        outbox.failNext = true;

        assertThatThrownBy(() -> gateway.submit(OrderRequest.limit("bob", "ACME", BUY, PRICE_100, 5)))
                .isInstanceOf(EngineHaltedException.class)
                .hasMessage(OrderGateway.HALTED_DETAIL)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(gateway.halt()).hasValueSatisfying(halt -> {
            assertThat(halt.reason()).isEqualTo("disk full");
            assertThat(halt.haltedAt()).isEqualTo(NOW);
        });

        BookSnapshot bookWhenHalted = gateway.book("ACME", 10);
        assertThatThrownBy(() -> gateway.submit(OrderRequest.limit("carol", "ACME", SELL, PRICE_100, 1)))
                .isInstanceOf(EngineHaltedException.class);
        assertThatThrownBy(() -> gateway.cancel("ACME", 1))
                .isInstanceOf(EngineHaltedException.class);
        assertThat(gateway.book("ACME", 10)).isEqualTo(bookWhenHalted);
        assertThat(outbox.batches).isEmpty();
    }

    private static final class RecordingOutboxWriter implements OutboxWriter {

        final List<List<TradeExecuted>> batches = new CopyOnWriteArrayList<>();
        volatile boolean failNext;

        @Override
        public void append(List<TradeExecuted> trades) {
            if (failNext) {
                failNext = false;
                throw new IllegalStateException("disk full");
            }
            batches.add(List.copyOf(trades));
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -B -pl order-service test -Dtest=OrderGatewayTest`
Expected: compilation fails, because `OrderGateway` has no constructor taking `OutboxWriter` and
`EngineHaltedException` does not exist.

- [ ] **Step 3: Create the exception and health indicator**

Create `order-service/src/main/java/dev/ledgerline/order/engine/EngineHaltedException.java`:

```java
package dev.ledgerline.order.engine;

/** Order entry is refused because trades could not be made durable. Only a restart clears it. */
public class EngineHaltedException extends RuntimeException {

    public EngineHaltedException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

Create `order-service/src/main/java/dev/ledgerline/order/engine/EngineHealthIndicator.java`:

```java
package dev.ledgerline.order.engine;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("matchingEngine")
class EngineHealthIndicator implements HealthIndicator {

    private final OrderGateway gateway;

    EngineHealthIndicator(OrderGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public Health health() {
        return gateway.halt()
                .map(halt -> Health.down()
                        .withDetail("reason", halt.reason())
                        .withDetail("haltedAt", halt.haltedAt().toString())
                        .build())
                .orElseGet(() -> Health.up().build());
    }
}
```

- [ ] **Step 4: Replace `OrderGateway`**

Replace `order-service/src/main/java/dev/ledgerline/order/engine/OrderGateway.java` with:

```java
package dev.ledgerline.order.engine;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.matching.BookSnapshot;
import dev.ledgerline.matching.MatchResult;
import dev.ledgerline.matching.MatchingEngine;
import dev.ledgerline.matching.OrderRequest;
import dev.ledgerline.matching.Trade;
import dev.ledgerline.order.outbox.OutboxWriter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Runs every engine command on one dedicated thread that owns the {@link MatchingEngine}.
 *
 * <p>Request threads never touch engine state; they hand commands to the sequencer and wait. This
 * single-writer design needs no locks and gives a deterministic order of events. A command's trades are
 * written to the outbox before the caller sees them. If that write fails, the in-memory book already
 * contains trades that are not durable, so order entry halts until restart instead of continuing on an
 * unrecorded state.
 */
@Component
public class OrderGateway implements DisposableBean {

    public record Halt(String reason, Instant haltedAt) {
    }

    static final String HALTED_DETAIL = "Order entry halted after a persistence failure; restart required";

    private static final Logger log = LoggerFactory.getLogger(OrderGateway.class);
    private static final Duration ENGINE_TIMEOUT = Duration.ofSeconds(5);

    private final MatchingEngine engine = new MatchingEngine();
    private final ExecutorService sequencer =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("matching-engine").factory());

    private final OutboxWriter outboxWriter;
    private final Clock clock;
    private final String currency;
    private final String sessionId;

    private volatile Halt halt;

    public OrderGateway(
            OutboxWriter outboxWriter, Clock clock, @Value("${ledgerline.currency:USD}") String currency) {
        this.outboxWriter = outboxWriter;
        this.clock = clock;
        this.currency = currency;
        // Engine trade ids restart at 1 on every boot, so qualify them to stay unique downstream.
        this.sessionId = Long.toString(clock.millis(), 36);
    }

    public OrderOutcome submit(OrderRequest request) {
        return onSequencer(() -> {
            ensureRunning();
            MatchResult result = engine.submit(request);
            List<TradeExecuted> trades = result.trades().stream().map(this::toEvent).toList();
            persist(trades);
            return new OrderOutcome(result, trades);
        });
    }

    public boolean cancel(String symbol, long orderId) {
        return onSequencer(() -> {
            ensureRunning();
            return engine.cancel(symbol, orderId);
        });
    }

    public BookSnapshot book(String symbol, int depth) {
        return onSequencer(() -> engine.snapshot(symbol, depth));
    }

    public Optional<Halt> halt() {
        return Optional.ofNullable(halt);
    }

    private void persist(List<TradeExecuted> trades) {
        if (trades.isEmpty()) {
            return;
        }
        try {
            outboxWriter.append(trades);
        } catch (RuntimeException e) {
            String reason = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            halt = new Halt(reason, clock.instant());
            log.error("Halting order entry: {} trade(s) could not be written to the outbox", trades.size(), e);
            throw new EngineHaltedException(HALTED_DETAIL, e);
        }
    }

    private void ensureRunning() {
        if (halt != null) {
            throw new EngineHaltedException(HALTED_DETAIL, null);
        }
    }

    private TradeExecuted toEvent(Trade trade) {
        return new TradeExecuted(
                "T-" + sessionId + "-" + trade.tradeId(),
                trade.symbol(),
                currency,
                Prices.fromTicks(trade.priceTicks()),
                trade.quantity(),
                trade.buyOrderId(),
                trade.buyAccountId(),
                trade.sellOrderId(),
                trade.sellAccountId(),
                trade.aggressorSide().name(),
                clock.instant());
    }

    private <T> T onSequencer(Callable<T> command) {
        try {
            return sequencer.submit(command).get(ENGINE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the matching engine", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("Matching engine command failed", e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException("Matching engine did not respond within " + ENGINE_TIMEOUT, e);
        }
    }

    @Override
    public void destroy() throws InterruptedException {
        sequencer.shutdown();
        if (!sequencer.awaitTermination(5, TimeUnit.SECONDS)) {
            sequencer.shutdownNow();
        }
    }
}
```

Delete the old publisher:

```bash
git rm order-service/src/main/java/dev/ledgerline/order/events/TradeEventPublisher.java order-service/src/main/java/dev/ledgerline/order/events/KafkaTradeEventPublisher.java
```

- [ ] **Step 5: Map the halt to 503**

In `order-service/src/main/java/dev/ledgerline/order/api/ApiExceptionHandler.java`, add the import
`import dev.ledgerline.order.engine.EngineHaltedException;` and this method after `handleInvalidPrice`:

```java
    @ExceptionHandler(EngineHaltedException.class)
    ProblemDetail handleHalted(EngineHaltedException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        problem.setTitle("Engine halted");
        return problem;
    }
```

- [ ] **Step 6: Run the unit test to verify it passes**

Run: `./mvnw -B -pl order-service test -Dtest=OrderGatewayTest`
Expected: 3 tests pass.

- [ ] **Step 7: Update the controller test to use the outbox**

Replace `order-service/src/test/java/dev/ledgerline/order/api/OrderControllerTest.java` with:

```java
package dev.ledgerline.order.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.order.PostgresTestConfiguration;
import dev.ledgerline.order.outbox.OutboxWriter;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Exercises the full stack from HTTP through the sequencer and the real matching engine, with the outbox
 * writer mocked. The engine is shared across tests, so each test uses its own symbol.
 */
@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@AutoConfigureMockMvc
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OutboxWriter outboxWriter;

    private ResultActions placeOrder(String json) throws Exception {
        return mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(json));
    }

    @Test
    void limitOrderOnEmptyBookRests() throws Exception {
        placeOrder("""
                {"accountId": "alice", "symbol": "RESTS", "side": "BUY", "type": "LIMIT", "price": 100.25, "quantity": 10}
                """)
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("NEW"))
                .andExpect(jsonPath("$.restingQuantity").value(10))
                .andExpect(jsonPath("$.fills").isEmpty());

        verify(outboxWriter, never()).append(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void crossingOrdersTradeAndAppendTheTradeToTheOutbox() throws Exception {
        placeOrder("""
                {"accountId": "alice", "symbol": "CROSS", "side": "SELL", "type": "LIMIT", "price": 101.50, "quantity": 5}
                """)
                .andExpect(status().isCreated());

        placeOrder("""
                {"accountId": "bob", "symbol": "CROSS", "side": "BUY", "type": "LIMIT", "price": 102, "quantity": 5}
                """)
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("FILLED"))
                .andExpect(jsonPath("$.fills.length()").value(1))
                .andExpect(jsonPath("$.fills[0].quantity").value(5));

        ArgumentCaptor<List<TradeExecuted>> appended = ArgumentCaptor.forClass(List.class);
        verify(outboxWriter).append(appended.capture());
        assertThat(appended.getValue()).singleElement().satisfies(trade -> {
            assertThat(trade.symbol()).isEqualTo("CROSS");
            assertThat(trade.price()).isEqualByComparingTo("101.50");
            assertThat(trade.quantity()).isEqualTo(5);
            assertThat(trade.buyAccountId()).isEqualTo("bob");
            assertThat(trade.sellAccountId()).isEqualTo("alice");
            assertThat(trade.currency()).isEqualTo("USD");
            assertThat(trade.aggressorSide()).isEqualTo("BUY");
        });
    }

    @Test
    void rejectsLimitOrderWithoutPrice() throws Exception {
        placeOrder("""
                {"accountId": "alice", "symbol": "NOPX", "side": "BUY", "type": "LIMIT", "quantity": 10}
                """)
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsMarketOrderWithPrice() throws Exception {
        placeOrder("""
                {"accountId": "alice", "symbol": "MKTPX", "side": "BUY", "type": "MARKET", "price": 10, "quantity": 10}
                """)
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsPriceFinerThanOneTick() throws Exception {
        placeOrder("""
                {"accountId": "alice", "symbol": "TICK", "side": "BUY", "type": "LIMIT", "price": 10.12345, "quantity": 10}
                """)
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Invalid price"));
    }

    @Test
    void cancelsARestingOrderOnlyOnce() throws Exception {
        String body = placeOrder("""
                {"accountId": "alice", "symbol": "CANCEL", "side": "SELL", "type": "LIMIT", "price": 50, "quantity": 1}
                """)
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long orderId = JsonPath.<Number>read(body, "$.orderId").longValue();

        mockMvc.perform(delete("/api/v1/orders/CANCEL/{orderId}", orderId)).andExpect(status().isNoContent());
        mockMvc.perform(delete("/api/v1/orders/CANCEL/{orderId}", orderId)).andExpect(status().isNotFound());
    }

    @Test
    void bookAggregatesDepthPerPriceLevel() throws Exception {
        placeOrder("""
                {"accountId": "a", "symbol": "DEPTH", "side": "BUY", "type": "LIMIT", "price": 99, "quantity": 3}
                """);
        placeOrder("""
                {"accountId": "b", "symbol": "DEPTH", "side": "BUY", "type": "LIMIT", "price": 99, "quantity": 4}
                """);
        placeOrder("""
                {"accountId": "c", "symbol": "DEPTH", "side": "SELL", "type": "LIMIT", "price": 101, "quantity": 2}
                """);

        mockMvc.perform(get("/api/v1/books/DEPTH"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bids.length()").value(1))
                .andExpect(jsonPath("$.bids[0].quantity").value(7))
                .andExpect(jsonPath("$.bids[0].orders").value(2))
                .andExpect(jsonPath("$.asks[0].quantity").value(2));
    }
}
```

- [ ] **Step 8: Write the halted-API test**

Create `order-service/src/test/java/dev/ledgerline/order/api/EngineHaltedApiTest.java`:

```java
package dev.ledgerline.order.api;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.order.PostgresTestConfiguration;
import dev.ledgerline.order.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Halting is permanent for the application context, so this test gets its own context. */
@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@AutoConfigureMockMvc
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext
class EngineHaltedApiTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OutboxWriter outboxWriter;

    @Test
    void outboxFailureReturns503ThenRejectsOrderEntryAndReportsHealthDown() throws Exception {
        doThrow(new IllegalStateException("disk full")).when(outboxWriter).append(anyList());

        mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"accountId": "alice", "symbol": "HALT", "side": "SELL", "type": "LIMIT", "price": 10, "quantity": 1}
                        """))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"accountId": "bob", "symbol": "HALT", "side": "BUY", "type": "LIMIT", "price": 10, "quantity": 1}
                        """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("Engine halted"))
                .andExpect(jsonPath("$.detail").value("Order entry halted after a persistence failure; restart required"));

        mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"accountId": "carol", "symbol": "HALT", "side": "SELL", "type": "LIMIT", "price": 11, "quantity": 1}
                        """))
                .andExpect(status().isServiceUnavailable());

        mockMvc.perform(get("/api/v1/books/HALT")).andExpect(status().isOk());

        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("DOWN"));
    }
}
```

- [ ] **Step 9: Run the module's tests**

Run: `./mvnw -B -pl order-service verify`
Expected:
- `OrderGatewayTest` passes 3 tests.
- `OrderControllerTest` passes 7 tests.
- `EngineHaltedApiTest` passes 1 test.
- `OutboxWriterIT` passes 3 tests.
- `BUILD SUCCESS`.

Also run `grep -r "TradeEventPublisher" order-service/src` and confirm there is no output.

- [ ] **Step 10: Commit**

```bash
git add -A order-service
git commit -m "Persist trades to the outbox before replying and halt order entry on failure"
```

### Task 3: Advisory-locked outbox relay

**Files:**
- Modify: `order-service/src/main/resources/application.yml` (Kafka producer section, relay properties)
- Modify: `order-service/src/main/java/dev/ledgerline/order/OrderServiceApplication.java` (add `@ConfigurationPropertiesScan`)
- Create: `order-service/src/main/java/dev/ledgerline/order/SchedulingConfiguration.java`
- Create: `order-service/src/main/java/dev/ledgerline/order/outbox/OutboxRelayProperties.java`
- Create: `order-service/src/main/java/dev/ledgerline/order/outbox/OutboxRelay.java`
- Create: `order-service/src/test/java/dev/ledgerline/order/outbox/OutboxRelayIT.java`
- Create: `order-service/src/test/java/dev/ledgerline/order/outbox/OutboxRelayFailureIT.java`
- Create: `order-service/src/test/java/dev/ledgerline/order/outbox/OutboxRelayLockIT.java`

**Interfaces:**
- Consumes:
  - `OutboxWriter.append(List<TradeExecuted>)` and `OutboxWriterIT.trade(String tradeId, String symbol)`
    (package-private static helper, same test package), both from Task 1.
  - `PostgresTestConfiguration`, `KafkaTestConfiguration` (Task 1).
  - Boot's `KafkaTemplate<String, String>` bean, configured by `application.yml` in this task.
- Produces:
  - `public int OutboxRelay.runOnce()`: number of rows published in this run.
  - `OutboxRelay.ADVISORY_LOCK_KEY = 7401001L`, `OutboxRelay.EVENT_TYPE_HEADER = "eventType"`.
  - `record OutboxRelayProperties(Duration pollInterval, int batchSize, Duration sendTimeout)`, bound to
    `ledgerline.outbox.relay`.
  - Property `ledgerline.scheduling.enabled`.
  - Metrics `ledgerline.outbox.pending`, `ledgerline.outbox.published`, `ledgerline.outbox.publish.failures`.

- [ ] **Step 1: Write the happy-path integration test**

Create `order-service/src/test/java/dev/ledgerline/order/outbox/OutboxRelayIT.java`:

```java
package dev.ledgerline.order.outbox;

import static dev.ledgerline.order.outbox.OutboxWriterIT.trade;
import static org.assertj.core.api.Assertions.assertThat;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.order.KafkaTestConfiguration;
import dev.ledgerline.order.PostgresTestConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = "ledgerline.scheduling.enabled=false")
@Import({PostgresTestConfiguration.class, KafkaTestConfiguration.class})
@Testcontainers(disabledWithoutDocker = true)
class OutboxRelayIT {

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private OutboxRelay relay;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private KafkaContainer kafka;

    @BeforeEach
    void clearOutbox() {
        jdbc.update("DELETE FROM order_service.outbox_events");
    }

    @Test
    void publishesPendingRowsInMatchOrderPerSymbolAndMarksThemPublished() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        List<TradeExecuted> matched = List.of(
                trade("T-" + run + "-1", "ACME"),
                trade("T-" + run + "-2", "GLOBX"),
                trade("T-" + run + "-3", "ACME"),
                trade("T-" + run + "-4", "GLOBX"),
                trade("T-" + run + "-5", "ACME"));
        matched.forEach(trade -> outboxWriter.append(List.of(trade)));

        assertThat(relay.runOnce()).isEqualTo(5);

        List<ConsumerRecord<String, String>> received = consumeTradesWithPrefix("T-" + run + "-", 5);
        Map<String, List<String>> tradeIdsBySymbol = received.stream().collect(Collectors.groupingBy(
                ConsumerRecord::key,
                Collectors.mapping(record -> tradeId(record.value()), Collectors.toList())));
        assertThat(tradeIdsBySymbol.get("ACME")).containsExactly("T-" + run + "-1", "T-" + run + "-3", "T-" + run + "-5");
        assertThat(tradeIdsBySymbol.get("GLOBX")).containsExactly("T-" + run + "-2", "T-" + run + "-4");
        assertThat(received).allSatisfy(record -> assertThat(
                new String(record.headers().lastHeader(OutboxRelay.EVENT_TYPE_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("TradeExecuted"));

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM order_service.outbox_events WHERE published_at IS NULL", Long.class)).isZero();
        assertThat(meterRegistry.get("ledgerline.outbox.pending").gauge().value()).isZero();
        assertThat(meterRegistry.get("ledgerline.outbox.published").counter().count()).isGreaterThanOrEqualTo(5);
    }

    @Test
    void runWithNothingPendingPublishesNothing() {
        assertThat(relay.runOnce()).isZero();
    }

    private String tradeId(String json) {
        return jsonMapper.readValue(json, TradeExecuted.class).tradeId();
    }

    private List<ConsumerRecord<String, String>> consumeTradesWithPrefix(String tradeIdPrefix, int expected) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-relay-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(Topics.TRADES_EXECUTED));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (received.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (tradeId(record.value()).startsWith(tradeIdPrefix)) {
                        received.add(record);
                    }
                }
            }
        }
        assertThat(received).hasSize(expected);
        return received;
    }
}
```

- [ ] **Step 2: Write the failure-path test**

Create `order-service/src/test/java/dev/ledgerline/order/outbox/OutboxRelayFailureIT.java`:

```java
package dev.ledgerline.order.outbox;

import static dev.ledgerline.order.outbox.OutboxWriterIT.trade;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.order.PostgresTestConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class OutboxRelayFailureIT {

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private OutboxRelay relay;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private MeterRegistry meterRegistry;

    @MockitoBean
    private KafkaTemplate<String, String> kafka;

    private final List<String> sentTradeIds = new CopyOnWriteArrayList<>();

    @BeforeEach
    void clearOutbox() {
        jdbc.update("DELETE FROM order_service.outbox_events");
    }

    @Test
    void stopsAtFirstFailedSendAndResumesFromThatRowInOrder() {
        for (int i = 1; i <= 5; i++) {
            outboxWriter.append(List.of(trade("T-f-" + i, "ACME")));
        }
        AtomicInteger calls = new AtomicInteger();
        when(kafka.send(ArgumentMatchers.<ProducerRecord<String, String>>any())).thenAnswer(invocation -> {
            recordSent(invocation.getArgument(0));
            return calls.incrementAndGet() == 3
                    ? CompletableFuture.failedFuture(new KafkaException("broker down"))
                    : CompletableFuture.completedFuture(null);
        });

        assertThat(relay.runOnce()).isEqualTo(2);

        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT published_at, attempts, last_error FROM order_service.outbox_events ORDER BY id");
        assertThat(rows).extracting(row -> row.get("published_at") != null)
                .containsExactly(true, true, false, false, false);
        assertThat(rows.get(2).get("attempts")).isEqualTo(1);
        assertThat((String) rows.get(2).get("last_error")).contains("broker down");
        assertThat(rows.get(3).get("attempts")).isEqualTo(0);
        assertThat(meterRegistry.get("ledgerline.outbox.pending").gauge().value()).isEqualTo(3.0);
        assertThat(meterRegistry.get("ledgerline.outbox.publish.failures").counter().count()).isEqualTo(1.0);

        reset(kafka);
        when(kafka.send(ArgumentMatchers.<ProducerRecord<String, String>>any())).thenAnswer(invocation -> {
            recordSent(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });

        assertThat(relay.runOnce()).isEqualTo(3);
        assertThat(sentTradeIds).containsExactly("T-f-1", "T-f-2", "T-f-3", "T-f-3", "T-f-4", "T-f-5");
        assertThat(meterRegistry.get("ledgerline.outbox.pending").gauge().value()).isZero();
    }

    private void recordSent(ProducerRecord<String, String> record) {
        sentTradeIds.add(jsonMapper.readValue(record.value(), TradeExecuted.class).tradeId());
    }
}
```

- [ ] **Step 3: Write the lock test**

Create `order-service/src/test/java/dev/ledgerline/order/outbox/OutboxRelayLockIT.java`:

```java
package dev.ledgerline.order.outbox;

import static dev.ledgerline.order.outbox.OutboxWriterIT.trade;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.ledgerline.order.PostgresTestConfiguration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class OutboxRelayLockIT {

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private OutboxRelay relay;

    @Autowired
    private JdbcTemplate jdbc;

    @MockitoBean
    private KafkaTemplate<String, String> kafka;

    @BeforeEach
    void clearOutbox() {
        jdbc.update("DELETE FROM order_service.outbox_events");
    }

    @Test
    void secondRelayRunSkipsWhileAnotherHoldsTheAdvisoryLock() throws Exception {
        outboxWriter.append(List.of(trade("T-l-1", "ACME")));
        CountDownLatch sendStarted = new CountDownLatch(1);
        CountDownLatch releaseSend = new CountDownLatch(1);
        when(kafka.send(ArgumentMatchers.<ProducerRecord<String, String>>any())).thenAnswer(invocation -> {
            sendStarted.countDown();
            releaseSend.await(10, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(null);
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> firstRun = executor.submit(relay::runOnce);
            assertThat(sendStarted.await(10, TimeUnit.SECONDS)).isTrue();

            int secondRun = relay.runOnce();
            releaseSend.countDown();

            assertThat(secondRun).isZero();
            assertThat(firstRun.get(10, TimeUnit.SECONDS)).isEqualTo(1);
            verify(kafka, times(1)).send(ArgumentMatchers.<ProducerRecord<String, String>>any());
        } finally {
            executor.shutdownNow();
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `./mvnw -B -pl order-service verify -Dit.test='OutboxRelay*IT' -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: test compilation fails with `cannot find symbol … class OutboxRelay`.

- [ ] **Step 5: Switch the producer to plain strings and add relay properties**

In `order-service/src/main/resources/application.yml`, replace the whole `spring.kafka` block with:

```yaml
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      properties:
        enable.idempotence: true
        # Bounds how long send() blocks for metadata while the broker is down (default 60 s),
        # so a relay run fails fast and retries instead of holding the advisory lock for a minute.
        max.block.ms: 5000
```

and replace the `ledgerline:` block with:

```yaml
ledgerline:
  currency: USD
  outbox:
    relay:
      poll-interval: 200ms
      batch-size: 100
      send-timeout: 5s
```

- [ ] **Step 6: Add properties, scheduling and the relay**

In `OrderServiceApplication.java`, add `@ConfigurationPropertiesScan` next to `@SpringBootApplication`, with
the import `org.springframework.boot.context.properties.ConfigurationPropertiesScan`.

Create `order-service/src/main/java/dev/ledgerline/order/SchedulingConfiguration.java`:

```java
package dev.ledgerline.order;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Tests turn scheduling off and drive the relay by calling {@code OutboxRelay.runOnce()} directly. */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(name = "ledgerline.scheduling.enabled", havingValue = "true", matchIfMissing = true)
class SchedulingConfiguration {
}
```

Create `order-service/src/main/java/dev/ledgerline/order/outbox/OutboxRelayProperties.java`:

```java
package dev.ledgerline.order.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("ledgerline.outbox.relay")
public record OutboxRelayProperties(
        @DefaultValue("200ms") Duration pollInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("5s") Duration sendTimeout) {
}
```

Create `order-service/src/main/java/dev/ledgerline/order/outbox/OutboxRelay.java`:

```java
package dev.ledgerline.order.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes outbox rows to Kafka in id order (which is match order).
 *
 * <p>Each run holds a transaction-scoped Postgres advisory lock, so only one relay publishes at a time
 * even with several order-service instances. Concurrent relays could interleave and reorder a symbol's
 * trades. A run stops at the first failed send, so no row is published ahead of an earlier one.
 * Delivery is at least once: a crash after the broker ack but before the commit republishes those
 * rows, and consumers de-duplicate by trade id.
 */
@Component
public class OutboxRelay {

    static final long ADVISORY_LOCK_KEY = 7401001L;
    static final String EVENT_TYPE_HEADER = "eventType";

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int MAX_ERROR_LENGTH = 1000;

    private static final String SELECT_BATCH = """
            SELECT id, topic, message_key, event_type, payload::text AS payload
            FROM order_service.outbox_events
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT ?""";

    private record OutboxRow(long id, String topic, String messageKey, String eventType, String payload) {
    }

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxRelayProperties properties;
    private final AtomicLong pending = new AtomicLong();
    private final Counter published;
    private final Counter failures;

    public OutboxRelay(
            JdbcTemplate jdbc,
            TransactionTemplate transactions,
            KafkaTemplate<String, String> kafka,
            OutboxRelayProperties properties,
            MeterRegistry meterRegistry) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.kafka = kafka;
        this.properties = properties;
        Gauge.builder("ledgerline.outbox.pending", pending, AtomicLong::get)
                .description("Outbox rows not yet published")
                .register(meterRegistry);
        this.published = Counter.builder("ledgerline.outbox.published").register(meterRegistry);
        this.failures = Counter.builder("ledgerline.outbox.publish.failures").register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${ledgerline.outbox.relay.poll-interval:200ms}")
    void scheduledRun() {
        try {
            runOnce();
        } catch (RuntimeException e) {
            log.warn("Outbox relay run failed; will retry on the next run", e);
        }
    }

    /** Publishes up to one batch of pending rows. Returns how many were published. */
    public int runOnce() {
        Integer publishedCount = transactions.execute(status -> publishBatch());
        pending.set(jdbc.queryForObject(
                "SELECT count(*) FROM order_service.outbox_events WHERE published_at IS NULL", Long.class));
        return publishedCount == null ? 0 : publishedCount;
    }

    private int publishBatch() {
        Boolean locked = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean.class, ADVISORY_LOCK_KEY);
        if (!Boolean.TRUE.equals(locked)) {
            return 0;
        }
        List<OutboxRow> rows = jdbc.query(SELECT_BATCH, (resultSet, rowNumber) -> new OutboxRow(
                resultSet.getLong("id"),
                resultSet.getString("topic"),
                resultSet.getString("message_key"),
                resultSet.getString("event_type"),
                resultSet.getString("payload")), properties.batchSize());

        List<Long> publishedIds = new ArrayList<>();
        for (OutboxRow row : rows) {
            try {
                send(row);
                publishedIds.add(row.id());
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                recordFailure(row, e);
                break;
            }
        }
        if (!publishedIds.isEmpty()) {
            jdbc.update("UPDATE order_service.outbox_events SET published_at = now() WHERE id = ANY(?)",
                    statement -> statement.setArray(1,
                            statement.getConnection().createArrayOf("bigint", publishedIds.toArray())));
            published.increment(publishedIds.size());
        }
        return publishedIds.size();
    }

    private void send(OutboxRow row) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.messageKey(), row.payload());
        record.headers().add(EVENT_TYPE_HEADER, row.eventType().getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void recordFailure(OutboxRow row, Exception e) {
        String message = rootCauseMessage(e);
        failures.increment();
        log.warn("Outbox row {} not published, will retry: {}", row.id(), message);
        jdbc.update("UPDATE order_service.outbox_events SET attempts = attempts + 1, last_error = ? WHERE id = ?",
                message, row.id());
    }

    static String rootCauseMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getClass().getSimpleName() + ": " + root.getMessage();
        return message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) : message;
    }
}
```

- [ ] **Step 7: Run the module's tests**

Run: `./mvnw -B -pl order-service verify`
Expected:
- `OutboxRelayIT` passes 2 tests.
- `OutboxRelayFailureIT` passes 1 test.
- `OutboxRelayLockIT` passes 1 test.
- Every Task 1 and Task 2 test still passes.
- `BUILD SUCCESS`.

- [ ] **Step 8: Commit**

```bash
git add -A order-service
git commit -m "Add advisory-locked outbox relay that publishes trades to Kafka in match order"
```

### Task 4: Settlement consumes plain JSON

**Files:**
- Modify (replace): `settlement-service/src/main/resources/application.yml`
- Create: `settlement-service/src/main/java/dev/ledgerline/settlement/messaging/InvalidTradeMessageException.java`
- Modify (replace): `settlement-service/src/main/java/dev/ledgerline/settlement/messaging/TradeListener.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/TradeJson.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/messaging/TradeListenerTest.java`

**Interfaces:**
- Consumes: `SettlementService.settle(TradeExecuted): boolean` (phase 1), the Boot `JsonMapper` bean.
- Produces:
  - `InvalidTradeMessageException(String message, Throwable cause)`
  - `TradeListener(SettlementService, JsonMapper)` with package-private
    `void onTradeExecuted(ConsumerRecord<String, String>)`
  - Test helper `TradeJson.valid(String tradeId): String`

- [ ] **Step 1: Write the test helper and failing test**

Create `settlement-service/src/test/java/dev/ledgerline/settlement/TradeJson.java`:

```java
package dev.ledgerline.settlement;

/** Wire-format JSON for a valid trade: bob buys 10 ACME from alice at 101.5. */
public final class TradeJson {

    private TradeJson() {
    }

    public static String valid(String tradeId) {
        return """
                {"tradeId":"%s","symbol":"ACME","currency":"USD","price":101.5,"quantity":10,\
                "buyOrderId":1,"buyAccountId":"bob","sellOrderId":2,"sellAccountId":"alice",\
                "aggressorSide":"BUY","executedAt":"2026-09-15T10:00:00Z"}""".formatted(tradeId);
    }
}
```

Create `settlement-service/src/test/java/dev/ledgerline/settlement/messaging/TradeListenerTest.java`:

```java
package dev.ledgerline.settlement.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.settlement.SettlementService;
import dev.ledgerline.settlement.TradeJson;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.json.JsonMapper;

class TradeListenerTest {

    private final SettlementService settlementService = mock(SettlementService.class);
    private final TradeListener listener = new TradeListener(settlementService, JsonMapper.builder().build());

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>(Topics.TRADES_EXECUTED, 0, 0L, "ACME", value);
    }

    @Test
    void validMessageIsParsedAndSettled() {
        listener.onTradeExecuted(record(TradeJson.valid("T-1")));

        ArgumentCaptor<TradeExecuted> settled = ArgumentCaptor.forClass(TradeExecuted.class);
        verify(settlementService).settle(settled.capture());
        assertThat(settled.getValue().tradeId()).isEqualTo("T-1");
        assertThat(settled.getValue().price()).isEqualByComparingTo("101.5");
        assertThat(settled.getValue().quantity()).isEqualTo(10);
        assertThat(settled.getValue().executedAt()).isEqualTo(Instant.parse("2026-09-15T10:00:00Z"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"bad\":",
            "not json",
            "{}",
            "{\"tradeId\":\"T-2\",\"symbol\":\"ACME\",\"currency\":\"USD\",\"quantity\":10,\"buyOrderId\":1,"
                    + "\"buyAccountId\":\"bob\",\"sellOrderId\":2,\"sellAccountId\":\"alice\","
                    + "\"aggressorSide\":\"BUY\",\"executedAt\":\"2026-09-15T10:00:00Z\"}"
    })
    void invalidMessagesAreRejectedWithoutSettling(String payload) {
        assertThatThrownBy(() -> listener.onTradeExecuted(record(payload)))
                .isInstanceOf(InvalidTradeMessageException.class);

        verifyNoInteractions(settlementService);
    }

    @Test
    void nullPayloadIsRejected() {
        assertThatThrownBy(() -> listener.onTradeExecuted(record(null)))
                .isInstanceOf(InvalidTradeMessageException.class);

        verifyNoInteractions(settlementService);
    }
}
```

(The last `@ValueSource` entry is a trade without `price`, which the `TradeExecuted` constructor rejects.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -B -pl settlement-service test -Dtest=TradeListenerTest`
Expected: compilation fails, because `InvalidTradeMessageException` does not exist and `TradeListener` has
no `(SettlementService, JsonMapper)` constructor.

- [ ] **Step 3: Implement**

Create `settlement-service/src/main/java/dev/ledgerline/settlement/messaging/InvalidTradeMessageException.java`:

```java
package dev.ledgerline.settlement.messaging;

/** The message can never be processed, so retrying is pointless and it goes straight to the dead-letter topic. */
public class InvalidTradeMessageException extends RuntimeException {

    public InvalidTradeMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

Replace `settlement-service/src/main/java/dev/ledgerline/settlement/messaging/TradeListener.java` with:

```java
package dev.ledgerline.settlement.messaging;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.settlement.SettlementService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumes trades as plain JSON. The wire contract is the JSON shape plus the {@code eventType} header, so no
 * Java class names cross service boundaries. Anything that cannot be parsed into a valid trade is marked as
 * permanently invalid.
 */
@Component
class TradeListener {

    private final SettlementService settlementService;
    private final JsonMapper jsonMapper;

    TradeListener(SettlementService settlementService, JsonMapper jsonMapper) {
        this.settlementService = settlementService;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = Topics.TRADES_EXECUTED, groupId = "${spring.application.name}")
    void onTradeExecuted(ConsumerRecord<String, String> record) {
        settlementService.settle(parse(record.value()));
    }

    private TradeExecuted parse(String payload) {
        if (payload == null) {
            throw new InvalidTradeMessageException("Empty trade message", null);
        }
        try {
            return jsonMapper.readValue(payload, TradeExecuted.class);
        } catch (JacksonException | IllegalArgumentException | NullPointerException e) {
            throw new InvalidTradeMessageException("Invalid TradeExecuted message: " + e.getMessage(), e);
        }
    }
}
```

Replace `settlement-service/src/main/resources/application.yml` with:

```yaml
server:
  port: 8082

spring:
  application:
    name: settlement-service
  datasource:
    url: jdbc:postgresql://localhost:5432/ledgerline
    username: ledgerline
    password: ledgerline
  jpa:
    open-in-view: false
    hibernate:
      # Flyway owns the schema.
      ddl-auto: none
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all

ledgerline:
  settlement:
    retry:
      initial-interval: 1s

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```

- [ ] **Step 4: Run the tests**

Run: `./mvnw -B -pl settlement-service verify`
Expected:
- `TradeListenerTest` passes 6 tests.
- `SettlementCalendarTest`, `TradePostingsTest` and `SettlementServiceIT` still pass.
- `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add -A settlement-service
git commit -m "Consume trades as plain JSON in settlement and flag unparseable messages"
```

---

### Task 5: Retries and dead-letter topic

**Files:**
- Modify: `ledgerline-events/src/main/java/dev/ledgerline/events/Topics.java`
- Modify: `settlement-service/pom.xml` (test dependencies)
- Create: `settlement-service/src/main/java/dev/ledgerline/settlement/messaging/KafkaConfiguration.java`
- Modify (replace): `settlement-service/src/main/java/dev/ledgerline/settlement/SettlementServiceApplication.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/PostgresTestConfiguration.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/KafkaTestConfiguration.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/KafkaProbe.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/messaging/DeadLetterIT.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/messaging/TransientRetryIT.java`

**Interfaces:**
- Consumes: `InvalidTradeMessageException` and `TradeJson.valid` (Task 4); Boot's `KafkaTemplate<String, String>`.
- Produces:
  - `Topics.TRADES_EXECUTED_DLT = "ledgerline.trades.executed.DLT"`
  - Beans `tradesExecutedTopic`, `tradesExecutedDeadLetterTopic` (3 partitions each) and `kafkaErrorHandler`
  - Test helpers `PostgresTestConfiguration`, `KafkaTestConfiguration` (bean `KafkaContainer kafka()`), and
    `KafkaProbe.poll(String bootstrapServers, String topic, Predicate<ConsumerRecord<String, String>> match, Duration timeout): Optional<ConsumerRecord<String, String>>`

- [ ] **Step 1: Add the topic constant and test dependencies**

Replace `ledgerline-events/src/main/java/dev/ledgerline/events/Topics.java` with:

```java
package dev.ledgerline.events;

public final class Topics {

    public static final String TRADES_EXECUTED = "ledgerline.trades.executed";
    public static final String TRADES_EXECUTED_DLT = TRADES_EXECUTED + ".DLT";

    private Topics() {
    }
}
```

In `settlement-service/pom.xml`, after the `testcontainers-postgresql` test dependency, add:

```xml
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: Create the test helpers**

Create `settlement-service/src/test/java/dev/ledgerline/settlement/PostgresTestConfiguration.java`:

```java
package dev.ledgerline.settlement;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;

@TestConfiguration(proxyBeanMethods = false)
public class PostgresTestConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:17-alpine");
    }
}
```

Create `settlement-service/src/test/java/dev/ledgerline/settlement/KafkaTestConfiguration.java`:

```java
package dev.ledgerline.settlement;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;

@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestConfiguration {

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer("apache/kafka:4.2.1");
    }
}
```

Create `settlement-service/src/test/java/dev/ledgerline/settlement/KafkaProbe.java`:

```java
package dev.ledgerline.settlement;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads a topic from the beginning with a throwaway consumer group until a matching record appears. */
public final class KafkaProbe {

    private KafkaProbe() {
    }

    public static Optional<ConsumerRecord<String, String>> poll(
            String bootstrapServers, String topic, Predicate<ConsumerRecord<String, String>> match, Duration timeout) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    if (match.test(record)) {
                        return Optional.of(record);
                    }
                }
            }
            return Optional.empty();
        }
    }
}
```

- [ ] **Step 3: Write the failing tests**

Create `settlement-service/src/test/java/dev/ledgerline/settlement/messaging/DeadLetterIT.java`:

```java
package dev.ledgerline.settlement.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.ledgerline.events.Topics;
import dev.ledgerline.settlement.KafkaProbe;
import dev.ledgerline.settlement.KafkaTestConfiguration;
import dev.ledgerline.settlement.PostgresTestConfiguration;
import dev.ledgerline.settlement.TradeJson;
import dev.ledgerline.settlement.persistence.SettlementInstructionRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest
@Import({PostgresTestConfiguration.class, KafkaTestConfiguration.class})
@Testcontainers(disabledWithoutDocker = true)
class DeadLetterIT {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private KafkaContainer kafka;

    @Autowired
    private SettlementInstructionRepository instructions;

    @BeforeEach
    void waitForListenerAssignment() {
        listeners.getListenerContainers().forEach(container -> ContainerTestUtils.waitForAssignment(container, 3));
    }

    @Test
    void malformedMessageGoesToDeadLetterTopicWithoutRetries() throws Exception {
        String key = "POISON-" + UUID.randomUUID().toString().substring(0, 8);
        long sentAt = System.nanoTime();

        SendResult<String, String> sent = kafkaTemplate.send(Topics.TRADES_EXECUTED, key, "{\"bad\":").get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> dead = KafkaProbe.poll(kafka.getBootstrapServers(), Topics.TRADES_EXECUTED_DLT,
                record -> key.equals(record.key()), Duration.ofSeconds(20)).orElseThrow();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - sentAt);

        assertThat(dead.value()).isEqualTo("{\"bad\":");
        assertThat(dead.partition()).isEqualTo(sent.getRecordMetadata().partition());
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_FQCN) + header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .contains("InvalidTradeMessageException");
        // With retries this would take at least 7 s (1 s + 2 s + 4 s).
        assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void validTradeBehindAPoisonMessageIsStillSettled() throws Exception {
        String tradeId = "T-dlt-" + UUID.randomUUID().toString().substring(0, 8);

        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", "{\"bad\":").get(10, TimeUnit.SECONDS);
        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", TradeJson.valid(tradeId)).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).until(() -> instructions.existsById(tradeId));
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
```

Create `settlement-service/src/test/java/dev/ledgerline/settlement/messaging/TransientRetryIT.java`:

```java
package dev.ledgerline.settlement.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.ledgerline.events.Topics;
import dev.ledgerline.settlement.KafkaProbe;
import dev.ledgerline.settlement.KafkaTestConfiguration;
import dev.ledgerline.settlement.PostgresTestConfiguration;
import dev.ledgerline.settlement.SettlementService;
import dev.ledgerline.settlement.TradeJson;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = "ledgerline.settlement.retry.initial-interval=100ms")
@Import({PostgresTestConfiguration.class, KafkaTestConfiguration.class})
@Testcontainers(disabledWithoutDocker = true)
class TransientRetryIT {

    @MockitoBean
    private SettlementService settlementService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private KafkaContainer kafka;

    @Test
    void transientFailureIsRetriedAndNotDeadLettered() throws Exception {
        when(settlementService.settle(any()))
                .thenThrow(new TransientDataAccessResourceException("db blip"))
                .thenThrow(new TransientDataAccessResourceException("db blip"))
                .thenReturn(true);
        listeners.getListenerContainers().forEach(container -> ContainerTestUtils.waitForAssignment(container, 3));
        String tradeId = "T-retry-" + UUID.randomUUID().toString().substring(0, 8);

        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", TradeJson.valid(tradeId)).get(10, TimeUnit.SECONDS);

        verify(settlementService, timeout(10_000).times(3)).settle(argThat(trade -> tradeId.equals(trade.tradeId())));
        assertThat(KafkaProbe.poll(kafka.getBootstrapServers(), Topics.TRADES_EXECUTED_DLT,
                record -> record.value() != null && record.value().contains(tradeId), Duration.ofSeconds(3)))
                .isEmpty();
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `./mvnw -B -pl ledgerline-events install -q && ./mvnw -B -pl settlement-service verify -Dit.test='DeadLetterIT,TransientRetryIT' -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: both ITs fail. Without Task 5's topic beans, `ledgerline.trades.executed` is auto-created with one
partition, so `ContainerTestUtils.waitForAssignment(container, 3)` fails. Even with the topic present, no
dead-letter topic exists and `orElseThrow()` throws `NoSuchElementException`, because the phase-1 handler logs
and skips failed records.

- [ ] **Step 5: Implement the error handling**

Create `settlement-service/src/main/java/dev/ledgerline/settlement/messaging/KafkaConfiguration.java`:

```java
package dev.ledgerline.settlement.messaging;

import dev.ledgerline.events.Topics;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Transient failures are retried with exponential backoff. Messages that can never succeed, and anything
 * still failing after the retries, go to the dead-letter topic on the same partition, instead of blocking
 * every later trade for that symbol.
 */
@Configuration(proxyBeanMethods = false)
class KafkaConfiguration {

    @Bean
    NewTopic tradesExecutedTopic() {
        return TopicBuilder.name(Topics.TRADES_EXECUTED).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic tradesExecutedDeadLetterTopic() {
        return TopicBuilder.name(Topics.TRADES_EXECUTED_DLT).partitions(3).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${ledgerline.settlement.retry.initial-interval:1s}") Duration initialInterval) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(initialInterval.toMillis());
        backOff.setMultiplier(2.0);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(InvalidTradeMessageException.class, IllegalStateException.class);
        return errorHandler;
    }
}
```

Replace `settlement-service/src/main/java/dev/ledgerline/settlement/SettlementServiceApplication.java` with:

```java
package dev.ledgerline.settlement;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SettlementServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SettlementServiceApplication.class, args);
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
```

- [ ] **Step 6: Run the tests**

Run: `./mvnw -B -pl ledgerline-events install -q && ./mvnw -B -pl settlement-service verify`
Expected:
- `DeadLetterIT` passes 2 tests.
- `TransientRetryIT` passes 1 test.
- `TradeListenerTest`, `SettlementServiceIT` and the unit tests still pass.
- `BUILD SUCCESS`.

- [ ] **Step 7: Commit**

```bash
git add -A ledgerline-events settlement-service
git commit -m "Retry transient settlement failures and dead-letter permanent ones"
```

### Task 6: Dead-letter ops API

**Files:**
- Modify: `settlement-service/pom.xml` (test dependency)
- Create: `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterRecord.java`
- Create: `settlement-service/src/main/java/dev/ledgerline/settlement/ops/ReplayResult.java`
- Create: `settlement-service/src/main/java/dev/ledgerline/settlement/ops/UnknownDeadLetterPartitionException.java`
- Create: `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterStore.java`
- Create: `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterReplayer.java`
- Create: `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterController.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/ops/DeadLetterOpsIT.java`

**Interfaces:**
- Consumes:
  - `Topics.TRADES_EXECUTED_DLT`, the DLT and error handler (Task 5), `TradeJson.valid` (Task 4).
  - `PostgresTestConfiguration` and `KafkaTestConfiguration` (Task 5).
  - Boot's `ConsumerFactory<String, String>` and `KafkaTemplate<String, String>`.
  - `JournalEntryRepository.findByTradeId(String)` and `SettlementInstructionRepository` (phase 1).
- Produces:
  - `GET /api/v1/ops/dead-letters?limit=N` returns `List<DeadLetterRecord>`.
  - `POST /api/v1/ops/dead-letters/{partition}/{offset}/replay` returns 202 `ReplayResult`, 404 if the
    record doesn't exist, 400 for an unknown partition.
  - `DeadLetterReplayer.REPLAYED_FROM_HEADER = "ledgerline-replayed-from"`.

- [ ] **Step 1: Add the MockMvc test dependency**

In `settlement-service/pom.xml`, after the `awaitility` test dependency, add:

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webmvc-test</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: Write the failing test**

Create `settlement-service/src/test/java/dev/ledgerline/settlement/ops/DeadLetterOpsIT.java`:

```java
package dev.ledgerline.settlement.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.events.Topics;
import dev.ledgerline.settlement.KafkaTestConfiguration;
import dev.ledgerline.settlement.PostgresTestConfiguration;
import dev.ledgerline.settlement.TradeJson;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import dev.ledgerline.settlement.persistence.SettlementInstructionRepository;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@AutoConfigureMockMvc
@Import({PostgresTestConfiguration.class, KafkaTestConfiguration.class})
@Testcontainers(disabledWithoutDocker = true)
class DeadLetterOpsIT {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private SettlementInstructionRepository instructions;

    @Autowired
    private JournalEntryRepository journal;

    @BeforeEach
    void waitForListenerAssignment() {
        listeners.getListenerContainers().forEach(container -> ContainerTestUtils.waitForAssignment(container, 3));
    }

    @Test
    void listsDeadLetteredRecordWithDiagnostics() throws Exception {
        String key = "POISON-" + UUID.randomUUID().toString().substring(0, 8);
        String match = "$[?(@.key == '" + key + "')]";

        kafkaTemplate.send(Topics.TRADES_EXECUTED, key, "{\"bad\":").get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> mockMvc.perform(get("/api/v1/ops/dead-letters?limit=500"))
                .andExpect(status().isOk())
                .andExpect(jsonPath(match + ".payload").value(hasItem("{\"bad\":")))
                .andExpect(jsonPath(match + ".exceptionClass").value(hasItem(containsString("InvalidTradeMessageException"))))
                .andExpect(jsonPath(match + ".originalOffset").value(hasItem(notNullValue())))
                .andExpect(jsonPath(match + ".failedAt").value(hasItem(notNullValue()))));
    }

    @Test
    void replayedDeadLetterIsBookedExactlyOnce() throws Exception {
        String tradeId = "T-ops-" + UUID.randomUUID().toString().substring(0, 8);
        SendResult<String, String> sent = kafkaTemplate
                .send(new ProducerRecord<>(Topics.TRADES_EXECUTED_DLT, 0, "ACME", TradeJson.valid(tradeId)))
                .get(10, TimeUnit.SECONDS);
        long offset = sent.getRecordMetadata().offset();

        mockMvc.perform(post("/api/v1/ops/dead-letters/0/{offset}/replay", offset))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.replayedTo").value(Topics.TRADES_EXECUTED))
                .andExpect(jsonPath("$.partition").value(0))
                .andExpect(jsonPath("$.offset").value(Math.toIntExact(offset)));

        await().atMost(Duration.ofSeconds(20)).until(() -> instructions.existsById(tradeId));
        assertThat(journal.findByTradeId(tradeId)).hasSize(4);

        mockMvc.perform(post("/api/v1/ops/dead-letters/0/{offset}/replay", offset)).andExpect(status().isAccepted());

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6))
                .untilAsserted(() -> assertThat(journal.findByTradeId(tradeId)).hasSize(4));
    }

    @Test
    void unknownOffsetIsNotFound() throws Exception {
        mockMvc.perform(post("/api/v1/ops/dead-letters/0/999999999/replay")).andExpect(status().isNotFound());
    }

    @Test
    void unknownPartitionIsBadRequest() throws Exception {
        mockMvc.perform(post("/api/v1/ops/dead-letters/99/0/replay"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Unknown dead-letter partition"));
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `./mvnw -B -pl settlement-service verify -Dit.test=DeadLetterOpsIT -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `listsDeadLetteredRecordWithDiagnostics` times out with a 404 status, and the replay tests fail
with 404 or 400 mismatches, because no ops controller exists yet.

- [ ] **Step 4: Implement the DTOs, store, replayer and controller**

Create `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterRecord.java`:

```java
package dev.ledgerline.settlement.ops;

import java.time.Instant;

public record DeadLetterRecord(
        int partition,
        long offset,
        String key,
        String payload,
        String exceptionClass,
        String exceptionMessage,
        Integer originalPartition,
        Long originalOffset,
        Instant failedAt) {
}
```

Create `settlement-service/src/main/java/dev/ledgerline/settlement/ops/ReplayResult.java`:

```java
package dev.ledgerline.settlement.ops;

/** Identifies the dead-letter record ({@code partition}, {@code offset}) that was republished to {@code replayedTo}. */
public record ReplayResult(String replayedTo, int partition, long offset) {
}
```

Create `settlement-service/src/main/java/dev/ledgerline/settlement/ops/UnknownDeadLetterPartitionException.java`:

```java
package dev.ledgerline.settlement.ops;

public class UnknownDeadLetterPartitionException extends RuntimeException {

    public UnknownDeadLetterPartitionException(int partition) {
        super("Dead-letter topic has no partition " + partition);
    }
}
```

Create `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterStore.java`:

```java
package dev.ledgerline.settlement.ops;

import dev.ledgerline.events.Topics;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Reads the dead-letter topic directly with a short-lived, group-less consumer. Nothing is committed, so
 * reading never moves any consumer group's position.
 */
@Component
public class DeadLetterStore {

    private static final Duration POLL_BUDGET = Duration.ofSeconds(5);

    private final ConsumerFactory<String, String> consumerFactory;

    DeadLetterStore(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    /** Up to {@code limit} most recent dead letters across all partitions, newest first. */
    public List<DeadLetterRecord> latest(int limit) {
        try (Consumer<String, String> consumer = newConsumer()) {
            List<TopicPartition> partitions = partitions(consumer);
            consumer.assign(partitions);
            Map<TopicPartition, Long> beginning = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, Math.max(beginning.get(partition), end.get(partition) - limit));
            }

            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            long deadline = System.nanoTime() + POLL_BUDGET.toNanos();
            while (!caughtUp(consumer, partitions, end) && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            records.sort(Comparator.comparingLong((ConsumerRecord<String, String> record) -> record.timestamp()).reversed());
            return records.stream().limit(limit).map(DeadLetterStore::toDeadLetter).toList();
        }
    }

    /** The record at exactly {@code partition}/{@code offset}, or empty if there is none. */
    public Optional<ConsumerRecord<String, String>> find(int partition, long offset) {
        try (Consumer<String, String> consumer = newConsumer()) {
            TopicPartition topicPartition = new TopicPartition(Topics.TRADES_EXECUTED_DLT, partition);
            if (!partitions(consumer).contains(topicPartition)) {
                throw new UnknownDeadLetterPartitionException(partition);
            }
            consumer.assign(List.of(topicPartition));
            long beginning = consumer.beginningOffsets(List.of(topicPartition)).get(topicPartition);
            long end = consumer.endOffsets(List.of(topicPartition)).get(topicPartition);
            if (offset < beginning || offset >= end) {
                return Optional.empty();
            }
            consumer.seek(topicPartition, offset);
            long deadline = System.nanoTime() + POLL_BUDGET.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.offset() == offset) {
                        return Optional.of(record);
                    }
                    if (record.offset() > offset) {
                        return Optional.empty();
                    }
                }
            }
            return Optional.empty();
        }
    }

    private Consumer<String, String> newConsumer() {
        Properties overrides = new Properties();
        overrides.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return consumerFactory.createConsumer(null, "dead-letter-store", null, overrides);
    }

    private static List<TopicPartition> partitions(Consumer<String, String> consumer) {
        return consumer.partitionsFor(Topics.TRADES_EXECUTED_DLT).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();
    }

    private static boolean caughtUp(
            Consumer<String, String> consumer, List<TopicPartition> partitions, Map<TopicPartition, Long> end) {
        return partitions.stream().allMatch(partition -> consumer.position(partition) >= end.get(partition));
    }

    private static DeadLetterRecord toDeadLetter(ConsumerRecord<String, String> record) {
        String causeClass = text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
        return new DeadLetterRecord(
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                causeClass != null ? causeClass : text(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                Instant.ofEpochMilli(record.timestamp()));
    }

    private static String text(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Integer intHeader(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value().length != Integer.BYTES ? null : ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long longHeader(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value().length != Long.BYTES ? null : ByteBuffer.wrap(header.value()).getLong();
    }
}
```

Create `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterReplayer.java`:

```java
package dev.ledgerline.settlement.ops;

import dev.ledgerline.events.Topics;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Republishes a dead letter to the trades topic. Safe to repeat because settlement is idempotent by trade id. */
@Component
class DeadLetterReplayer {

    static final String REPLAYED_FROM_HEADER = "ledgerline-replayed-from";

    private final DeadLetterStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;

    DeadLetterReplayer(DeadLetterStore store, KafkaTemplate<String, String> kafkaTemplate) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
    }

    Optional<ReplayResult> replay(int partition, long offset) {
        return store.find(partition, offset).map(this::republish);
    }

    private ReplayResult republish(ConsumerRecord<String, String> deadLetter) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(Topics.TRADES_EXECUTED, deadLetter.key(), deadLetter.value());
        String origin = Topics.TRADES_EXECUTED_DLT + "/" + deadLetter.partition() + "/" + deadLetter.offset();
        record.headers().add(REPLAYED_FROM_HEADER, origin.getBytes(StandardCharsets.UTF_8));
        try {
            kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while replaying " + origin, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Replay of " + origin + " was not acknowledged", e);
        }
        return new ReplayResult(Topics.TRADES_EXECUTED, deadLetter.partition(), deadLetter.offset());
    }
}
```

Create `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterController.java`:

```java
package dev.ledgerline.settlement.ops;

import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/ops/dead-letters")
class DeadLetterController {

    private static final int MAX_LIMIT = 500;

    private final DeadLetterStore store;
    private final DeadLetterReplayer replayer;

    DeadLetterController(DeadLetterStore store, DeadLetterReplayer replayer) {
        this.store = store;
        this.replayer = replayer;
    }

    @GetMapping
    List<DeadLetterRecord> list(@RequestParam(defaultValue = "50") int limit) {
        return store.latest(Math.clamp(limit, 1, MAX_LIMIT));
    }

    @PostMapping("/{partition}/{offset}/replay")
    ResponseEntity<ReplayResult> replay(@PathVariable int partition, @PathVariable long offset) {
        return replayer.replay(partition, offset)
                .map(result -> ResponseEntity.accepted().body(result))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @ExceptionHandler(UnknownDeadLetterPartitionException.class)
    ProblemDetail handleUnknownPartition(UnknownDeadLetterPartitionException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
        problem.setTitle("Unknown dead-letter partition");
        return problem;
    }
}
```

- [ ] **Step 5: Run the tests**

Run: `./mvnw -B -pl settlement-service verify`
Expected:
- `DeadLetterOpsIT` passes 4 tests.
- Every Task 4 and Task 5 test and every phase-1 settlement test still passes.
- `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add -A settlement-service
git commit -m "Add ops API to list and replay dead-lettered trades"
```

---

### Task 7: Demos, README and acceptance run

**Files:**
- Create: `scripts/demo-kafka-outage.sh`
- Create: `scripts/demo-poison-message.sh`
- Modify: `README.md`

**Interfaces:**
- Consumes: every endpoint and metric from Tasks 1–6; `docker-compose.yml` services `kafka` and `postgres`.
- Produces: runnable demos and documentation. No code interfaces.

- [ ] **Step 1: Write the Kafka-outage demo**

Create `scripts/demo-kafka-outage.sh`:

```bash
#!/usr/bin/env bash
# Orders keep working while Kafka is down; the outbox delivers the trades once Kafka is back.
# Needs: docker compose stack running, order-service on 8081, settlement-service on 8082.
set -euo pipefail

DOCKER="${DOCKER:-docker}"
ORDER_URL="${ORDER_URL:-http://localhost:8081}"
SETTLEMENT_URL="${SETTLEMENT_URL:-http://localhost:8082}"
SYMBOL="$(LC_ALL=C tr -dc 'A-Z' </dev/urandom | head -c 6)"
JSON='Content-Type: application/json'

pending() {
  curl -s "$ORDER_URL/actuator/metrics/ledgerline.outbox.pending" | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["measurements"][0]["value"]))'
}

echo "== Stopping Kafka"
"$DOCKER" compose stop kafka

echo "== Crossing orders for $SYMBOL while Kafka is down"
curl -s -o /dev/null -w "sell: HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$JSON" \
  -d "{\"accountId\":\"outage-seller\",\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":50,\"quantity\":5}"
curl -s -o /dev/null -w "buy:  HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$JSON" \
  -d "{\"accountId\":\"outage-buyer\",\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"price\":50,\"quantity\":5}"
sleep 2
echo "outbox pending while Kafka is down: $(pending)"

echo "== Starting Kafka"
"$DOCKER" compose start kafka
for _ in $(seq 1 60); do
  [ "$(pending)" = "0" ] && break
  sleep 1
done
echo "outbox pending after Kafka is back: $(pending)"

for _ in $(seq 1 30); do
  curl -s "$SETTLEMENT_URL/api/v1/accounts/outage-buyer/balances" | grep -q "$SYMBOL" && break
  sleep 1
done
echo "== outage-buyer balances (look for $SYMBOL)"
curl -s "$SETTLEMENT_URL/api/v1/accounts/outage-buyer/balances"; echo
```

- [ ] **Step 2: Write the poison-message demo**

Create `scripts/demo-poison-message.sh`:

```bash
#!/usr/bin/env bash
# A malformed trade message is dead-lettered instead of blocking settlement; a valid dead letter can be replayed.
# Needs: docker compose stack running, settlement-service on 8082.
set -euo pipefail

DOCKER="${DOCKER:-docker}"
SETTLEMENT_URL="${SETTLEMENT_URL:-http://localhost:8082}"
TRADE_ID="T-demo-$(date +%s)"

produce() { # topic, key, value
  printf '%s|%s\n' "$2" "$3" | "$DOCKER" compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$1" --property parse.key=true --property 'key.separator=|'
}

echo "== Publishing a malformed trade"
produce ledgerline.trades.executed POISON '{"bad":'
sleep 3
echo "== Latest dead letters"
curl -s "$SETTLEMENT_URL/api/v1/ops/dead-letters?limit=3"; echo

echo "== Putting a valid trade ($TRADE_ID) on the dead-letter topic, then replaying it"
produce ledgerline.trades.executed.DLT ACME "{\"tradeId\":\"$TRADE_ID\",\"symbol\":\"ACME\",\"currency\":\"USD\",\"price\":101.5,\"quantity\":10,\"buyOrderId\":1,\"buyAccountId\":\"replay-buyer\",\"sellOrderId\":2,\"sellAccountId\":\"replay-seller\",\"aggressorSide\":\"BUY\",\"executedAt\":\"2026-09-15T10:00:00Z\"}"
sleep 2
COORDINATES="$(curl -s "$SETTLEMENT_URL/api/v1/ops/dead-letters?limit=50" | python3 -c "
import json, sys
for record in json.load(sys.stdin):
    if '$TRADE_ID' in (record['payload'] or ''):
        print(record['partition'], record['offset']); break
")"
read -r PARTITION OFFSET <<< "$COORDINATES"
echo "found at partition $PARTITION offset $OFFSET"
curl -s -X POST "$SETTLEMENT_URL/api/v1/ops/dead-letters/$PARTITION/$OFFSET/replay"; echo
sleep 3
echo "== replay-buyer balances (ACME +10, USD -1015)"
curl -s "$SETTLEMENT_URL/api/v1/accounts/replay-buyer/balances"; echo
```

Make both executable:

```bash
chmod +x scripts/demo-kafka-outage.sh scripts/demo-poison-message.sh
```

- [ ] **Step 3: Update the README**

In `README.md`, replace the architecture block:

```
Client ──REST──► order-service ──────────────► Kafka: ledgerline.trades.executed ──► settlement-service ──► PostgreSQL
                  │  single-threaded sequencer        (keyed by symbol)                 │  idempotent booking
                  └─► matching-engine (in-memory)                                         └─ double-entry journal, T+1
```

with:

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

In the "Design decisions" list, replace the bullet that starts with `- **Exactly-once effect over at-least-once delivery.**` with:

```markdown
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
```

After the "Try it" section, add:

````markdown
### Failure demos

```bash
DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker ./scripts/demo-kafka-outage.sh     # orders succeed while Kafka is down; trades delivered after
DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker ./scripts/demo-poison-message.sh   # bad message dead-lettered; valid dead letter replayed
```

Outbox backlog: `curl localhost:8081/actuator/metrics/ledgerline.outbox.pending`
````

In "Testing", add this bullet after the Order service bullet:

```markdown
- **Outbox and dead letters:** Testcontainers tests cover atomic batch writes, halt on write failure, in-order
  publishing, resuming after a failed send, single relay under the advisory lock, dead-lettering without retries,
  retrying transient failures, and replay booking exactly once.
```

In "Roadmap", replace the Phase 2 line with:

```markdown
- [ ] **Phase 2:**
  - [x] A. Transactional outbox, fail-stop engine, dead-letter topic and replay API
  - [ ] C. Real-time risk and P&L with Kafka Streams
  - [ ] B. OAuth2 with Keycloak and roles (trader, risk, ops)
  - [ ] D. Live React dashboard over WebSocket
```

- [ ] **Step 4: Run the full build**

Run: `./mvnw -B clean verify`
Expected: `BUILD SUCCESS`, and every test in every module passes with none skipped (Docker is running).

- [ ] **Step 5: Manual acceptance run (spec §8)**

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
docker compose up -d kafka kafka-ui postgres
java -Xmx384m -jar order-service/target/order-service-0.1.0-SNAPSHOT.jar &
java -Xmx384m -jar settlement-service/target/settlement-service-0.1.0-SNAPSHOT.jar &
```

Once both `/actuator/health` endpoints return `UP`:
1. **Criteria 1 and 2:** run `./scripts/demo-kafka-outage.sh`. Both orders return 201; pending > 0 while Kafka is
   down; pending reaches 0 within 10 s of restart; the buyer's balance shows the symbol.
2. **Criteria 4 and 6:** run `./scripts/demo-poison-message.sh`. The malformed record is listed; replay returns 202;
   `replay-buyer` shows ACME +10.
3. **Criterion 3:** `docker compose stop postgres`, then POST two crossing orders. Expect 503 `Engine halted`, and
   `curl -i localhost:8081/actuator/health` returns 503 `DOWN`. Then `docker compose start postgres` and restart
   order-service; health returns to `UP`.
4. **Criterion 5:** already covered by `TransientRetryIT` in Step 4.

Record the observed output of each step in the commit message body. Then stop the services and
`docker compose stop`.

- [ ] **Step 6: Commit**

```bash
git add scripts README.md
git commit -m "Document outbox and dead-letter design with runnable failure demos"
```

Ask the user before pushing (`git push origin main`). Once pushed, confirm the GitHub Actions run is green.
