# Risk and P&L Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new `risk-service` that turns the live trade stream into per-account, per-symbol positions
with average-cost realised and unrealised P&L, raises an alert the first time a position goes over its
notional limit, and serves positions over REST.

**Architecture:**
- **Pipeline:** Kafka Streams reads `ledgerline.trades.executed` as plain JSON strings, parses them, and
  drops duplicate trade ids with a state store. Each trade becomes two fills (buyer and seller).
- **Stores:** fills are aggregated into a `positions` table. A separate table keeps the last traded price
  per symbol.
- **Alerts:** limit crossings from the positions table go to `ledgerline.risk.alerts`.
- **API:** a Spring MVC endpoint reads the local state stores (interactive queries).

**Tech Stack:** Java 21, Spring Boot 4.1.1, Spring Kafka 4.1.1 (`@EnableKafkaStreams`, `JacksonJsonSerde`),
Kafka Streams 4.2.1 (`processValues` / `FixedKeyProcessor`, RocksDB stores), Jackson 3, JUnit 5,
`kafka-streams-test-utils` (`TopologyTestDriver`), Testcontainers 2.0.5.

**Spec:** `docs/superpowers/specs/2026-09-15-phase-2-design.md`, section **C** (and section A for the wire
format it must follow).

## Global Constraints

- **Repo and build:** repo root `/Users/adityagupta/Developer/ledgerline`; build with `./mvnw`. No dependency
  versions outside the Spring Boot 4.1.1 BOM.
- **Module and runtime:** module `risk-service`, base package `dev.ledgerline.risk`, HTTP port **8083**,
  Kafka Streams `application.id` **`risk-service`**, `processing.guarantee` **`exactly_once_v2`**.
- **Input topic:** `ledgerline.trades.executed`. Values are plain JSON strings (the sub-project A wire
  format, no type headers), keyed by symbol.
- **Output topic:** `ledgerline.risk.alerts`. Values are plain JSON `RiskAlert` with no type headers, keyed
  `accountId|symbol`.
- **State store names:** `seen-trades`, `positions`, `last-prices`. The position key format is
  `accountId|symbol`.
- **Money:** `BigDecimal`, scale 4, `RoundingMode.HALF_EVEN`. Never `double`.
- **Properties:** `ledgerline.risk.position-notional-limit` (default `1000000`),
  `ledgerline.risk.dedupe-retention` (default `7d`).
- **Endpoint:** `GET /api/v1/risk/accounts/{accountId}/positions`. Returns 503 ProblemDetail with title
  `Risk data unavailable` until Kafka Streams is `RUNNING`.
- **Docker-backed tests:** `@Testcontainers(disabledWithoutDocker = true)`, image `apache/kafka:4.2.1`. The
  Docker CLI is `/Applications/Docker.app/Contents/Resources/bin/docker` and is not on PATH.
- **Commits:** one per task, no attribution lines. `./mvnw -B verify` must be green before each commit.
- **Deliberate deviations from spec section C:**
  - The input is read with a String serde and parsed inside the de-duplication processor. Unparseable
    messages are logged and skipped, which settlement already dead-letters. This replaces
    `JacksonJsonSerde<TradeExecuted>`.
  - `seen-trades` is a key-value store cleaned by an hourly wall-clock punctuator, not a `WindowStore`.
  - The `positions` store has **caching disabled**. With caching on, two fills inside one commit interval
    collapse into one update and a limit crossing can be lost.
  - The API's `notional` and `unrealizedPnl` use the symbol's last traded price (mark price) from
    `last-prices`, not the account's own last fill.

## File Structure

**Repo root and events module**

| File | Responsibility |
|---|---|
| `pom.xml` | Adds the `risk-service` module |
| `ledgerline-events/…/events/Topics.java` | Adds `RISK_ALERTS` |
| `ledgerline-events/…/events/RiskAlert.java` | Alert event contract |

**risk-service main code**

| File | Responsibility |
|---|---|
| `pom.xml` | Dependencies and plugins |
| `src/main/resources/application.yml` | Port, Kafka Streams and risk properties |
| `…/risk/RiskServiceApplication.java` | Boot entry point, `@ConfigurationPropertiesScan` |
| `…/risk/RiskProperties.java` | Typed `ledgerline.risk.*` properties |
| `…/risk/domain/Fill.java` | One side of a trade, signed quantity |
| `…/risk/domain/Position.java` | Average-cost position maths, pure Java |
| `…/risk/stream/JsonSerdes.java` | Plain-JSON serdes without type headers |
| `…/risk/stream/DeduplicatingTradeParser.java` | Parse, drop invalid messages and duplicate trade ids, evict old ids |
| `…/risk/stream/RiskTopology.java` | Builds the whole topology onto a `StreamsBuilder` |
| `…/risk/stream/RiskStreamsConfiguration.java` | `@EnableKafkaStreams`, topics, wires the topology |
| `…/risk/query/RiskStores.java` | Read access to the state stores (interface) |
| `…/risk/query/KafkaStreamsRiskStores.java` | Production implementation via `StreamsBuilderFactoryBean` |
| `…/risk/query/RiskDataUnavailableException.java` | Streams not yet queryable |
| `…/risk/query/PositionView.java` | API view with mark price and unrealised P&L |
| `…/risk/query/PositionQueryService.java` | Prefix scan plus mark-to-market |
| `…/risk/api/PositionController.java` | REST endpoint and 503 mapping |

**risk-service tests**

| File | Responsibility |
|---|---|
| `…/risk/domain/PositionTest.java` | P&L maths unit tests |
| `…/risk/TopologyFixture.java` | `TopologyTestDriver` wrapper shared by tests |
| `…/risk/stream/RiskTopologyTest.java` | Topology behaviour |
| `…/risk/query/PositionQueryServiceTest.java` | Query and mark-to-market against real stores |
| `…/risk/api/PositionControllerTest.java` | HTTP mapping and 503 |
| `…/risk/KafkaTestConfiguration.java` | Kafka container bean |
| `…/risk/RiskServiceIT.java` | End to end with real Kafka |

**Docs and scripts**

| File | Responsibility |
|---|---|
| `scripts/demo-risk.sh` | Runnable demo |
| `README.md` | Updated docs |

---

### Task 1: Module skeleton and position maths

**Files:**
- Modify: `pom.xml` (root, `<modules>`)
- Create: `risk-service/pom.xml`
- Create: `risk-service/src/main/resources/application.yml`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/RiskServiceApplication.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/RiskProperties.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/domain/Fill.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/domain/Position.java`
- Test: `risk-service/src/test/java/dev/ledgerline/risk/domain/PositionTest.java`

**Interfaces:**
- Consumes: `dev.ledgerline.events.TradeExecuted` (phase 1).
- Produces:
  - `public record Fill(String tradeId, String accountId, String symbol, long signedQuantity, BigDecimal price, Instant executedAt)`
    with `static Fill buyerSide(TradeExecuted)` and `static Fill sellerSide(TradeExecuted)`.
  - `public record Position(String accountId, String symbol, long netQuantity, BigDecimal averageCost, BigDecimal realizedPnl, BigDecimal lastPrice, BigDecimal notional, String lastTradeId, boolean limitBreachedByLastFill)`
    with `static Position empty()`, `Position apply(Fill fill, BigDecimal notionalLimit)`,
    `BigDecimal unrealizedPnl(BigDecimal markPrice)` and `Position.SCALE = 4`.
  - `public record RiskProperties(BigDecimal positionNotionalLimit, Duration dedupeRetention)`, bound to
    `ledgerline.risk`.

- [ ] **Step 1: Register the module and create the POM**

In the root `pom.xml`, change `<modules>` to:

```xml
    <modules>
        <module>ledgerline-events</module>
        <module>matching-engine</module>
        <module>order-service</module>
        <module>settlement-service</module>
        <module>risk-service</module>
    </modules>
```

Create `risk-service/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>dev.ledgerline</groupId>
        <artifactId>ledgerline-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>risk-service</artifactId>
    <name>Ledgerline :: Risk Service</name>
    <description>Real-time positions, average-cost P&amp;L and limit alerts with Kafka Streams</description>

    <dependencies>
        <dependency>
            <groupId>dev.ledgerline</groupId>
            <artifactId>ledgerline-events</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webmvc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webmvc-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams-test-utils</artifactId>
            <scope>test</scope>
        </dependency>
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
            <artifactId>testcontainers-kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Application class, properties and configuration**

Create `risk-service/src/main/java/dev/ledgerline/risk/RiskServiceApplication.java`:

```java
package dev.ledgerline.risk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class RiskServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RiskServiceApplication.class, args);
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/RiskProperties.java`:

```java
package dev.ledgerline.risk;

import java.math.BigDecimal;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param positionNotionalLimit an alert is raised the first time a position's notional goes above this
 * @param dedupeRetention       how long processed trade ids are remembered for de-duplication
 */
@ConfigurationProperties("ledgerline.risk")
public record RiskProperties(
        @DefaultValue("1000000") BigDecimal positionNotionalLimit,
        @DefaultValue("7d") Duration dedupeRetention) {
}
```

Create `risk-service/src/main/resources/application.yml`:

```yaml
server:
  port: 8083

spring:
  application:
    name: risk-service
  mvc:
    problemdetails:
      enabled: true
  kafka:
    bootstrap-servers: localhost:9092
    streams:
      application-id: risk-service
      replication-factor: 1
      state-dir: ${java.io.tmpdir}/ledgerline/risk-service
      properties:
        processing.guarantee: exactly_once_v2
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
        default.value.serde: org.apache.kafka.common.serialization.Serdes$StringSerde

ledgerline:
  risk:
    position-notional-limit: 1000000
    dedupe-retention: 7d

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```

- [ ] **Step 3: Write the failing test**

Create `risk-service/src/test/java/dev/ledgerline/risk/domain/PositionTest.java`:

```java
package dev.ledgerline.risk.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PositionTest {

    private static final BigDecimal NO_LIMIT = new BigDecimal("1000000000");

    private static Fill fill(long signedQuantity, String price) {
        return new Fill("T-" + signedQuantity + "@" + price, "alice", "ACME", signedQuantity,
                new BigDecimal(price), Instant.parse("2026-09-16T10:00:00Z"));
    }

    private static Position positionAfter(Fill... fills) {
        Position position = Position.empty();
        for (Fill fill : fills) {
            position = position.apply(fill, NO_LIMIT);
        }
        return position;
    }

    @Test
    void openingALongSetsAverageCostToTheFillPrice() {
        Position position = positionAfter(fill(10, "100"));

        assertThat(position.accountId()).isEqualTo("alice");
        assertThat(position.symbol()).isEqualTo("ACME");
        assertThat(position.netQuantity()).isEqualTo(10);
        assertThat(position.averageCost()).isEqualByComparingTo("100");
        assertThat(position.realizedPnl()).isEqualByComparingTo("0");
        assertThat(position.lastPrice()).isEqualByComparingTo("100");
        assertThat(position.notional()).isEqualByComparingTo("1000");
        assertThat(position.lastTradeId()).isEqualTo("T-10@100");
    }

    @Test
    void addingToALongWeightsTheAverageCost() {
        Position position = positionAfter(fill(10, "100"), fill(10, "110"));

        assertThat(position.netQuantity()).isEqualTo(20);
        assertThat(position.averageCost()).isEqualByComparingTo("105");
        assertThat(position.realizedPnl()).isEqualByComparingTo("0");
    }

    @Test
    void reducingALongRealisesProfitAgainstAverageCost() {
        Position position = positionAfter(fill(20, "105"), fill(-5, "120"));

        assertThat(position.netQuantity()).isEqualTo(15);
        assertThat(position.averageCost()).isEqualByComparingTo("105");
        assertThat(position.realizedPnl()).isEqualByComparingTo("75");
    }

    @Test
    void reducingAShortRealisesALossWhenThePriceRose() {
        Position position = positionAfter(fill(-10, "50"), fill(4, "55"));

        assertThat(position.netQuantity()).isEqualTo(-6);
        assertThat(position.averageCost()).isEqualByComparingTo("50");
        assertThat(position.realizedPnl()).isEqualByComparingTo("-20");
    }

    @Test
    void crossingThroughZeroClosesTheOldPositionAndOpensTheRestAtTheFillPrice() {
        Position position = positionAfter(fill(10, "100"), fill(-15, "90"));

        assertThat(position.netQuantity()).isEqualTo(-5);
        assertThat(position.averageCost()).isEqualByComparingTo("90");
        assertThat(position.realizedPnl()).isEqualByComparingTo("-100");
        assertThat(position.notional()).isEqualByComparingTo("450");
    }

    @Test
    void closingFlatResetsAverageCostAndKeepsRealisedPnl() {
        Position position = positionAfter(fill(10, "100"), fill(-10, "101"));

        assertThat(position.netQuantity()).isZero();
        assertThat(position.averageCost()).isEqualByComparingTo("0");
        assertThat(position.realizedPnl()).isEqualByComparingTo("10");
        assertThat(position.notional()).isEqualByComparingTo("0");
    }

    @Test
    void flagsTheLimitOnlyOnTheFillThatCrossesIt() {
        BigDecimal limit = new BigDecimal("1000");

        Position atLimit = Position.empty().apply(fill(10, "100"), limit);
        Position crossed = atLimit.apply(fill(1, "100"), limit);
        Position stillAbove = crossed.apply(fill(1, "100"), limit);
        Position backBelow = stillAbove.apply(fill(-5, "100"), limit);
        Position crossedAgain = backBelow.apply(fill(5, "100"), limit);

        assertThat(atLimit.limitBreachedByLastFill()).as("notional 1000 is not above 1000").isFalse();
        assertThat(crossed.limitBreachedByLastFill()).as("1000 -> 1100").isTrue();
        assertThat(stillAbove.limitBreachedByLastFill()).as("1100 -> 1200").isFalse();
        assertThat(backBelow.limitBreachedByLastFill()).as("1200 -> 700").isFalse();
        assertThat(crossedAgain.limitBreachedByLastFill()).as("700 -> 1200").isTrue();
    }

    @Test
    void unrealisedPnlMarksLongsAndShortsAgainstAverageCost() {
        assertThat(positionAfter(fill(10, "100")).unrealizedPnl(new BigDecimal("103.5"))).isEqualByComparingTo("35");
        assertThat(positionAfter(fill(-10, "50")).unrealizedPnl(new BigDecimal("45"))).isEqualByComparingTo("50");
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `./mvnw -B -pl risk-service -am test -Dtest=PositionTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation fails with `cannot find symbol … class Fill` and `class Position`.

- [ ] **Step 5: Implement `Fill` and `Position`**

Create `risk-service/src/main/java/dev/ledgerline/risk/domain/Fill.java`:

```java
package dev.ledgerline.risk.domain;

import dev.ledgerline.events.TradeExecuted;
import java.math.BigDecimal;
import java.time.Instant;

/** One account's side of a trade: positive quantity for the buyer, negative for the seller. */
public record Fill(String tradeId, String accountId, String symbol, long signedQuantity, BigDecimal price, Instant executedAt) {

    public static Fill buyerSide(TradeExecuted trade) {
        return new Fill(trade.tradeId(), trade.buyAccountId(), trade.symbol(), trade.quantity(), trade.price(),
                trade.executedAt());
    }

    public static Fill sellerSide(TradeExecuted trade) {
        return new Fill(trade.tradeId(), trade.sellAccountId(), trade.symbol(), -trade.quantity(), trade.price(),
                trade.executedAt());
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/domain/Position.java`:

```java
package dev.ledgerline.risk.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Net position of one account in one symbol, with average-cost P&L.
 *
 * <p>Average cost changes only when a position is opened or increased. Reducing a position realises P&L
 * against the average cost. Going through zero closes the old position, and the remainder opens at the
 * fill price.
 */
public record Position(
        String accountId,
        String symbol,
        long netQuantity,
        BigDecimal averageCost,
        BigDecimal realizedPnl,
        BigDecimal lastPrice,
        BigDecimal notional,
        String lastTradeId,
        boolean limitBreachedByLastFill) {

    public static final int SCALE = 4;

    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE);

    public static Position empty() {
        return new Position(null, null, 0, ZERO, ZERO, ZERO, ZERO, null, false);
    }

    public Position apply(Fill fill, BigDecimal notionalLimit) {
        BigDecimal price = fill.price().setScale(SCALE, RoundingMode.HALF_EVEN);
        long delta = fill.signedQuantity();
        long newQuantity = Math.addExact(netQuantity, delta);

        BigDecimal newAverageCost = averageCost;
        BigDecimal newRealizedPnl = realizedPnl;
        if (netQuantity == 0 || Long.signum(netQuantity) == Long.signum(delta)) {
            BigDecimal totalCost = averageCost.multiply(BigDecimal.valueOf(Math.abs(netQuantity)))
                    .add(price.multiply(BigDecimal.valueOf(Math.abs(delta))));
            newAverageCost = totalCost.divide(BigDecimal.valueOf(Math.abs(newQuantity)), SCALE, RoundingMode.HALF_EVEN);
        } else {
            long closedQuantity = Math.min(Math.abs(netQuantity), Math.abs(delta));
            newRealizedPnl = realizedPnl.add(price.subtract(averageCost)
                    .multiply(BigDecimal.valueOf(closedQuantity * Long.signum(netQuantity))));
            if (newQuantity == 0) {
                newAverageCost = ZERO;
            } else if (Long.signum(newQuantity) != Long.signum(netQuantity)) {
                newAverageCost = price;
            }
        }

        BigDecimal newNotional = price.multiply(BigDecimal.valueOf(Math.abs(newQuantity)));
        boolean crossedLimit = notional.compareTo(notionalLimit) <= 0 && newNotional.compareTo(notionalLimit) > 0;
        return new Position(fill.accountId(), fill.symbol(), newQuantity, newAverageCost, newRealizedPnl, price,
                newNotional, fill.tradeId(), crossedLimit);
    }

    /** P&L if the whole position were closed at {@code markPrice}. */
    public BigDecimal unrealizedPnl(BigDecimal markPrice) {
        return markPrice.subtract(averageCost).multiply(BigDecimal.valueOf(netQuantity));
    }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `./mvnw -B -pl risk-service -am verify`
Expected: `PositionTest` passes 8 tests, and `BUILD SUCCESS`.

(`verify` also packages the Spring Boot app. The app has no Kafka Streams topology yet, so no test starts
a context.)

- [ ] **Step 7: Commit**

```bash
git add pom.xml risk-service
git commit -m "Add risk-service module with average-cost position maths"
```

---

### Task 2: Kafka Streams topology

**Files:**
- Modify: `ledgerline-events/src/main/java/dev/ledgerline/events/Topics.java`
- Create: `ledgerline-events/src/main/java/dev/ledgerline/events/RiskAlert.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/stream/JsonSerdes.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/stream/DeduplicatingTradeParser.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/stream/RiskTopology.java`
- Test: `risk-service/src/test/java/dev/ledgerline/risk/TopologyFixture.java`
- Test: `risk-service/src/test/java/dev/ledgerline/risk/stream/RiskTopologyTest.java`

**Interfaces:**
- Consumes: `Fill`, `Position`, `RiskProperties` (Task 1); `TradeExecuted`, `Topics.TRADES_EXECUTED`.
- Produces:
  - `Topics.RISK_ALERTS = "ledgerline.risk.alerts"`
  - `public record RiskAlert(String accountId, String symbol, long netQuantity, BigDecimal notional, BigDecimal limit, String tradeId)`
  - `public final class RiskTopology` with constructor `RiskTopology(JsonMapper, RiskProperties)`,
    `void addTo(StreamsBuilder)`, `static String positionKey(String accountId, String symbol)`, and constants
    `SEEN_TRADES_STORE`, `POSITIONS_STORE`, `LAST_PRICES_STORE`.
  - `JsonSerdes.of(Class<T>, JsonMapper): Serde<T>`
  - Test helper `TopologyFixture` (`AutoCloseable`) with:
    - constructor `TopologyFixture(BigDecimal limit, Path stateDir)`
    - `void pipeTrade(String tradeId, String buyer, String seller, String symbol, long quantity, String price)`
    - `void pipeRaw(String value)`
    - `KeyValueStore<String, Position> positions()`, `KeyValueStore<String, String> lastPrices()`
    - `List<RiskAlert> readAlerts()`
    - `void advanceWallClock(Duration)`

- [ ] **Step 1: Add the alert contract**

Replace `ledgerline-events/src/main/java/dev/ledgerline/events/Topics.java` with:

```java
package dev.ledgerline.events;

public final class Topics {

    public static final String TRADES_EXECUTED = "ledgerline.trades.executed";
    public static final String TRADES_EXECUTED_DLT = TRADES_EXECUTED + ".DLT";
    public static final String RISK_ALERTS = "ledgerline.risk.alerts";

    private Topics() {
    }
}
```

Create `ledgerline-events/src/main/java/dev/ledgerline/events/RiskAlert.java`:

```java
package dev.ledgerline.events;

import java.math.BigDecimal;

/** Published by the risk service the first time a position's notional goes above its limit. */
public record RiskAlert(
        String accountId,
        String symbol,
        long netQuantity,
        BigDecimal notional,
        BigDecimal limit,
        String tradeId) {
}
```

- [ ] **Step 2: Write the test fixture and failing tests**

Create `risk-service/src/test/java/dev/ledgerline/risk/TopologyFixture.java`:

```java
package dev.ledgerline.risk;

import dev.ledgerline.events.RiskAlert;
import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.risk.domain.Position;
import dev.ledgerline.risk.stream.RiskTopology;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import tools.jackson.databind.json.JsonMapper;

/** Runs the real risk topology in-process with {@link TopologyTestDriver}: no broker, no Docker. */
public final class TopologyFixture implements AutoCloseable {

    public static final JsonMapper JSON = JsonMapper.builder().build();

    private final TopologyTestDriver driver;
    private final TestInputTopic<String, String> trades;
    private final TestOutputTopic<String, String> alerts;

    public TopologyFixture(BigDecimal limit, Path stateDir) {
        StreamsBuilder builder = new StreamsBuilder();
        new RiskTopology(JSON, new RiskProperties(limit, Duration.ofDays(7))).addTo(builder);
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "risk-topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(builder.build(), config, Instant.parse("2026-09-16T10:00:00Z"));
        trades = driver.createInputTopic(Topics.TRADES_EXECUTED, new StringSerializer(), new StringSerializer());
        alerts = driver.createOutputTopic(Topics.RISK_ALERTS, new StringDeserializer(), new StringDeserializer());
    }

    public void pipeTrade(String tradeId, String buyer, String seller, String symbol, long quantity, String price) {
        TradeExecuted trade = new TradeExecuted(tradeId, symbol, "USD", new BigDecimal(price), quantity,
                1, buyer, 2, seller, "BUY", Instant.parse("2026-09-16T10:00:00Z"));
        trades.pipeInput(symbol, JSON.writeValueAsString(trade));
    }

    public void pipeRaw(String value) {
        trades.pipeInput("RAW", value);
    }

    public KeyValueStore<String, Position> positions() {
        return driver.getKeyValueStore(RiskTopology.POSITIONS_STORE);
    }

    public KeyValueStore<String, String> lastPrices() {
        return driver.getKeyValueStore(RiskTopology.LAST_PRICES_STORE);
    }

    public List<RiskAlert> readAlerts() {
        return alerts.readValuesToList().stream().map(json -> JSON.readValue(json, RiskAlert.class)).toList();
    }

    public void advanceWallClock(Duration duration) {
        driver.advanceWallClockTime(duration);
    }

    @Override
    public void close() {
        driver.close();
    }
}
```

Create `risk-service/src/test/java/dev/ledgerline/risk/stream/RiskTopologyTest.java`:

```java
package dev.ledgerline.risk.stream;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledgerline.events.RiskAlert;
import dev.ledgerline.risk.TopologyFixture;
import dev.ledgerline.risk.domain.Position;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RiskTopologyTest {

    private TopologyFixture fixture;

    @BeforeEach
    void startTopology(@TempDir Path stateDir) {
        fixture = new TopologyFixture(new BigDecimal("1000"), stateDir);
    }

    @AfterEach
    void stopTopology() {
        fixture.close();
    }

    private Position position(String accountId, String symbol) {
        return fixture.positions().get(RiskTopology.positionKey(accountId, symbol));
    }

    @Test
    void aTradeUpdatesTheBuyerAndSellerPositions() {
        fixture.pipeTrade("T-1", "bob", "alice", "ACME", 5, "100");

        assertThat(position("bob", "ACME").netQuantity()).isEqualTo(5);
        assertThat(position("bob", "ACME").averageCost()).isEqualByComparingTo("100");
        assertThat(position("alice", "ACME").netQuantity()).isEqualTo(-5);
        assertThat(position("alice", "ACME").averageCost()).isEqualByComparingTo("100");
    }

    @Test
    void aRedeliveredTradeIsCountedOnce() {
        fixture.pipeTrade("T-dup", "bob", "alice", "ACME", 5, "100");
        fixture.pipeTrade("T-dup", "bob", "alice", "ACME", 5, "100");

        assertThat(position("bob", "ACME").netQuantity()).isEqualTo(5);
        assertThat(position("alice", "ACME").netQuantity()).isEqualTo(-5);
    }

    @Test
    void tradeIdsAreForgottenAfterTheRetentionPeriod() {
        fixture.pipeTrade("T-old", "bob", "alice", "ACME", 5, "100");

        fixture.advanceWallClock(Duration.ofDays(8));
        fixture.pipeTrade("T-old", "bob", "alice", "ACME", 5, "100");

        assertThat(position("bob", "ACME").netQuantity()).isEqualTo(10);
    }

    @Test
    void crossingTheLimitRaisesOneAlertPerPositionOnlyOnce() {
        fixture.pipeTrade("T-a", "bob", "alice", "ACME", 10, "100");
        assertThat(fixture.readAlerts()).as("notional 1000 is not above the limit").isEmpty();

        fixture.pipeTrade("T-b", "bob", "alice", "ACME", 1, "100");
        fixture.pipeTrade("T-c", "bob", "alice", "ACME", 1, "100");

        assertThat(fixture.readAlerts())
                .extracting(RiskAlert::accountId, RiskAlert::symbol, RiskAlert::netQuantity, RiskAlert::tradeId)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("bob", "ACME", 11L, "T-b"),
                        org.assertj.core.groups.Tuple.tuple("alice", "ACME", -11L, "T-b"));
    }

    @Test
    void alertCarriesTheNotionalAndTheLimit() {
        fixture.pipeTrade("T-big", "bob", "alice", "GLOBX", 2, "600");

        assertThat(fixture.readAlerts()).hasSize(2).allSatisfy(alert -> {
            assertThat(alert.notional()).isEqualByComparingTo("1200");
            assertThat(alert.limit()).isEqualByComparingTo("1000");
        });
    }

    @Test
    void lastPriceIsTheMostRecentTradePerSymbol() {
        fixture.pipeTrade("T-p1", "bob", "alice", "ACME", 1, "100");
        fixture.pipeTrade("T-p2", "carol", "dave", "ACME", 1, "101.5");
        fixture.pipeTrade("T-p3", "bob", "alice", "GLOBX", 1, "20");

        assertThat(new BigDecimal(fixture.lastPrices().get("ACME"))).isEqualByComparingTo("101.5");
        assertThat(new BigDecimal(fixture.lastPrices().get("GLOBX"))).isEqualByComparingTo("20");
    }

    @Test
    void unparseableMessagesAreSkippedWithoutStoppingTheStream() {
        fixture.pipeRaw("{\"bad\":");
        fixture.pipeRaw("not json");
        fixture.pipeTrade("T-after-bad", "bob", "alice", "ACME", 3, "100");

        assertThat(position("bob", "ACME").netQuantity()).isEqualTo(3);
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `./mvnw -B -pl risk-service -am test -Dtest=RiskTopologyTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation fails with `cannot find symbol … class RiskTopology`.

- [ ] **Step 4: Implement the serdes, de-duplicating parser and topology**

Create `risk-service/src/main/java/dev/ledgerline/risk/stream/JsonSerdes.java`:

```java
package dev.ledgerline.risk.stream;

import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

/** Plain-JSON serdes: no type headers are written or read, so no Java class names go over the wire. */
final class JsonSerdes {

    private JsonSerdes() {
    }

    static <T> Serde<T> of(Class<T> type, JsonMapper jsonMapper) {
        return new JacksonJsonSerde<T>(type, jsonMapper).noTypeInfo().ignoreTypeHeaders();
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/stream/DeduplicatingTradeParser.java`:

```java
package dev.ledgerline.risk.stream;

import dev.ledgerline.events.TradeExecuted;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Parses trade JSON and drops trade ids it has already seen.
 *
 * <p>{@code exactly_once_v2} stops Kafka Streams from applying one input record twice, but the order service's
 * outbox relay delivers at least once. The same trade can therefore arrive as two different records, and
 * without this step it would be counted twice. Seen ids are evicted after the retention period by an hourly
 * wall-clock punctuator. Messages that are not valid trades are logged and skipped; settlement already
 * dead-letters them.
 */
final class DeduplicatingTradeParser implements FixedKeyProcessor<String, String, TradeExecuted> {

    private static final Logger log = LoggerFactory.getLogger(DeduplicatingTradeParser.class);
    private static final Duration EVICTION_INTERVAL = Duration.ofHours(1);

    private final JsonMapper jsonMapper;
    private final Duration retention;

    private FixedKeyProcessorContext<String, TradeExecuted> context;
    private KeyValueStore<String, Long> seenTrades;

    DeduplicatingTradeParser(JsonMapper jsonMapper, Duration retention) {
        this.jsonMapper = jsonMapper;
        this.retention = retention;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, TradeExecuted> context) {
        this.context = context;
        this.seenTrades = context.getStateStore(RiskTopology.SEEN_TRADES_STORE);
        context.schedule(EVICTION_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::evictExpired);
    }

    @Override
    public void process(FixedKeyRecord<String, String> record) {
        TradeExecuted trade = parse(record.value());
        if (trade == null) {
            return;
        }
        if (seenTrades.get(trade.tradeId()) != null) {
            log.info("Ignoring redelivered trade {}", trade.tradeId());
            return;
        }
        seenTrades.put(trade.tradeId(), context.currentSystemTimeMs());
        context.forward(record.withValue(trade));
    }

    private TradeExecuted parse(String payload) {
        if (payload == null) {
            log.warn("Skipping empty trade message");
            return null;
        }
        try {
            return jsonMapper.readValue(payload, TradeExecuted.class);
        } catch (JacksonException | IllegalArgumentException | NullPointerException e) {
            log.warn("Skipping invalid trade message: {}", e.getMessage());
            return null;
        }
    }

    private void evictExpired(long now) {
        long cutoff = now - retention.toMillis();
        List<String> expired = new ArrayList<>();
        try (KeyValueIterator<String, Long> entries = seenTrades.all()) {
            while (entries.hasNext()) {
                KeyValue<String, Long> entry = entries.next();
                if (entry.value < cutoff) {
                    expired.add(entry.key);
                }
            }
        }
        expired.forEach(seenTrades::delete);
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/stream/RiskTopology.java`:

```java
package dev.ledgerline.risk.stream;

import dev.ledgerline.events.RiskAlert;
import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.risk.RiskProperties;
import dev.ledgerline.risk.domain.Fill;
import dev.ledgerline.risk.domain.Position;
import java.math.BigDecimal;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import tools.jackson.databind.json.JsonMapper;

/**
 * trades → de-duplicate → two fills per trade → positions per account and symbol → limit alerts, plus the
 * last traded price per symbol.
 */
public final class RiskTopology {

    public static final String SEEN_TRADES_STORE = "seen-trades";
    public static final String POSITIONS_STORE = "positions";
    public static final String LAST_PRICES_STORE = "last-prices";

    private final JsonMapper jsonMapper;
    private final RiskProperties properties;

    public RiskTopology(JsonMapper jsonMapper, RiskProperties properties) {
        this.jsonMapper = jsonMapper;
        this.properties = properties;
    }

    public static String positionKey(String accountId, String symbol) {
        return accountId + "|" + symbol;
    }

    public void addTo(StreamsBuilder builder) {
        Serde<Fill> fillSerde = JsonSerdes.of(Fill.class, jsonMapper);
        Serde<Position> positionSerde = JsonSerdes.of(Position.class, jsonMapper);
        Serde<RiskAlert> alertSerde = JsonSerdes.of(RiskAlert.class, jsonMapper);
        BigDecimal limit = properties.positionNotionalLimit();

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(SEEN_TRADES_STORE), Serdes.String(), Serdes.Long()));

        KStream<String, TradeExecuted> trades = builder
                .stream(Topics.TRADES_EXECUTED, Consumed.with(Serdes.String(), Serdes.String()))
                .processValues(() -> new DeduplicatingTradeParser(jsonMapper, properties.dedupeRetention()),
                        SEEN_TRADES_STORE);

        KTable<String, Position> positions = trades
                .flatMap((symbol, trade) -> List.of(
                        KeyValue.pair(positionKey(trade.buyAccountId(), trade.symbol()), Fill.buyerSide(trade)),
                        KeyValue.pair(positionKey(trade.sellAccountId(), trade.symbol()), Fill.sellerSide(trade))))
                .groupByKey(Grouped.with("fills-by-position", Serdes.String(), fillSerde))
                .aggregate(Position::empty, (key, fill, position) -> position.apply(fill, limit),
                        Materialized.<String, Position, KeyValueStore<Bytes, byte[]>>as(POSITIONS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(positionSerde)
                                // Every update must reach the alert filter. With caching, two fills inside one
                                // commit interval would collapse into one update and a limit crossing could be lost.
                                .withCachingDisabled());

        positions.toStream()
                .filter((key, position) -> position != null && position.limitBreachedByLastFill())
                .mapValues(position -> new RiskAlert(position.accountId(), position.symbol(), position.netQuantity(),
                        position.notional(), limit, position.lastTradeId()))
                .to(Topics.RISK_ALERTS, Produced.with(Serdes.String(), alertSerde));

        trades
                .selectKey((key, trade) -> trade.symbol())
                .mapValues(trade -> trade.price().toPlainString())
                .groupByKey(Grouped.with("prices-by-symbol", Serdes.String(), Serdes.String()))
                .reduce((previous, latest) -> latest,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(LAST_PRICES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()));
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./mvnw -B -pl risk-service -am verify`
Expected: `RiskTopologyTest` passes 7 tests, `PositionTest` still passes 8, and `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add ledgerline-events risk-service
git commit -m "Build risk topology: de-duplicated trades to positions, last prices and limit alerts"
```

### Task 3: Positions query API and Spring wiring

**Files:**
- Create: `risk-service/src/main/java/dev/ledgerline/risk/stream/RiskStreamsConfiguration.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/query/RiskStores.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/query/KafkaStreamsRiskStores.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/query/RiskDataUnavailableException.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/query/PositionView.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/query/PositionQueryService.java`
- Create: `risk-service/src/main/java/dev/ledgerline/risk/api/PositionController.java`
- Test: `risk-service/src/test/java/dev/ledgerline/risk/query/PositionQueryServiceTest.java`
- Test: `risk-service/src/test/java/dev/ledgerline/risk/api/PositionControllerTest.java`

**Interfaces:**
- Consumes:
  - `RiskTopology` (constructor, `addTo`, store names) and `TopologyFixture` (`pipeTrade`, `positions()`,
    `lastPrices()`), both from Task 2.
  - `Position`, `RiskProperties` (Task 1).
- Produces:
  - `public interface RiskStores { ReadOnlyKeyValueStore<String, Position> positions(); ReadOnlyKeyValueStore<String, String> lastPrices(); }`
  - `public record PositionView(String symbol, long netQuantity, BigDecimal averageCost, BigDecimal lastPrice, BigDecimal notional, BigDecimal realizedPnl, BigDecimal unrealizedPnl)`
  - `public class PositionQueryService` with `List<PositionView> positionsFor(String accountId)`
  - `public class RiskDataUnavailableException extends RuntimeException` (constructor `(String message)`)
  - Endpoint `GET /api/v1/risk/accounts/{accountId}/positions`

- [ ] **Step 1: Write the failing tests**

Create `risk-service/src/test/java/dev/ledgerline/risk/query/PositionQueryServiceTest.java`:

```java
package dev.ledgerline.risk.query;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledgerline.risk.TopologyFixture;
import dev.ledgerline.risk.domain.Position;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PositionQueryServiceTest {

    private TopologyFixture fixture;
    private PositionQueryService service;

    @BeforeEach
    void setUp(@TempDir Path stateDir) {
        fixture = new TopologyFixture(new BigDecimal("1000000"), stateDir);
        service = new PositionQueryService(new RiskStores() {
            @Override
            public ReadOnlyKeyValueStore<String, Position> positions() {
                return fixture.positions();
            }

            @Override
            public ReadOnlyKeyValueStore<String, String> lastPrices() {
                return fixture.lastPrices();
            }
        });
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void marksEachPositionAtTheSymbolsLastTradedPrice() {
        fixture.pipeTrade("T-1", "alice", "bob", "ACME", 10, "100");
        fixture.pipeTrade("T-2", "carol", "dave", "ACME", 5, "110");
        fixture.pipeTrade("T-3", "alice", "bob", "GLOBX", 4, "20");

        List<PositionView> alice = service.positionsFor("alice");

        assertThat(alice).extracting(PositionView::symbol).containsExactly("ACME", "GLOBX");
        PositionView acme = alice.get(0);
        assertThat(acme.netQuantity()).isEqualTo(10);
        assertThat(acme.averageCost()).isEqualByComparingTo("100");
        assertThat(acme.lastPrice()).as("carol's later trade moved the ACME price").isEqualByComparingTo("110");
        assertThat(acme.notional()).isEqualByComparingTo("1100");
        assertThat(acme.realizedPnl()).isEqualByComparingTo("0");
        assertThat(acme.unrealizedPnl()).isEqualByComparingTo("100");
        assertThat(alice.get(1).unrealizedPnl()).isEqualByComparingTo("0");
    }

    @Test
    void shortPositionsLoseWhenThePriceRises() {
        fixture.pipeTrade("T-1", "alice", "bob", "ACME", 10, "100");
        fixture.pipeTrade("T-2", "carol", "dave", "ACME", 1, "110");

        assertThat(service.positionsFor("bob")).singleElement().satisfies(position -> {
            assertThat(position.netQuantity()).isEqualTo(-10);
            assertThat(position.unrealizedPnl()).isEqualByComparingTo("-100");
        });
    }

    @Test
    void accountIdIsMatchedExactlyNotByPrefix() {
        fixture.pipeTrade("T-1", "alice", "bob", "ACME", 1, "100");

        assertThat(service.positionsFor("al")).isEmpty();
        assertThat(service.positionsFor("nobody")).isEmpty();
    }
}
```

Create `risk-service/src/test/java/dev/ledgerline/risk/api/PositionControllerTest.java`:

```java
package dev.ledgerline.risk.api;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.risk.query.PositionQueryService;
import dev.ledgerline.risk.query.PositionView;
import dev.ledgerline.risk.query.RiskDataUnavailableException;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PositionController.class)
class PositionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private PositionQueryService queryService;

    @Test
    void returnsTheAccountsPositions() throws Exception {
        when(queryService.positionsFor("alice")).thenReturn(List.of(new PositionView("ACME", 10,
                new BigDecimal("100.0000"), new BigDecimal("110.0000"), new BigDecimal("1100.0000"),
                new BigDecimal("0.0000"), new BigDecimal("100.0000"))));

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].symbol").value("ACME"))
                .andExpect(jsonPath("$[0].netQuantity").value(10))
                .andExpect(jsonPath("$[0].averageCost").value(100.0))
                .andExpect(jsonPath("$[0].lastPrice").value(110.0))
                .andExpect(jsonPath("$[0].notional").value(1100.0))
                .andExpect(jsonPath("$[0].unrealizedPnl").value(100.0));
    }

    @Test
    void returns503WhileRiskDataIsNotQueryable() throws Exception {
        when(queryService.positionsFor("alice")).thenThrow(new RiskDataUnavailableException("stream state: REBALANCING"));

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("Risk data unavailable"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./mvnw -B -pl risk-service -am test -Dtest='PositionQueryServiceTest,PositionControllerTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation fails with `cannot find symbol … class PositionQueryService`.

- [ ] **Step 3: Implement the query layer**

Create `risk-service/src/main/java/dev/ledgerline/risk/query/RiskStores.java`:

```java
package dev.ledgerline.risk.query;

import dev.ledgerline.risk.domain.Position;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** Read access to the risk state stores. Throws {@link RiskDataUnavailableException} while they are not queryable. */
public interface RiskStores {

    ReadOnlyKeyValueStore<String, Position> positions();

    ReadOnlyKeyValueStore<String, String> lastPrices();
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/query/RiskDataUnavailableException.java`:

```java
package dev.ledgerline.risk.query;

public class RiskDataUnavailableException extends RuntimeException {

    public RiskDataUnavailableException(String message) {
        super(message);
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/query/KafkaStreamsRiskStores.java`:

```java
package dev.ledgerline.risk.query;

import dev.ledgerline.risk.domain.Position;
import dev.ledgerline.risk.stream.RiskTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/** Interactive queries against this instance's local stores. Single instance only; see README. */
@Component
class KafkaStreamsRiskStores implements RiskStores {

    private final StreamsBuilderFactoryBean streamsFactory;

    KafkaStreamsRiskStores(StreamsBuilderFactoryBean streamsFactory) {
        this.streamsFactory = streamsFactory;
    }

    @Override
    public ReadOnlyKeyValueStore<String, Position> positions() {
        return store(RiskTopology.POSITIONS_STORE);
    }

    @Override
    public ReadOnlyKeyValueStore<String, String> lastPrices() {
        return store(RiskTopology.LAST_PRICES_STORE);
    }

    private <V> ReadOnlyKeyValueStore<String, V> store(String name) {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            throw new RiskDataUnavailableException(
                    "Risk data is not available yet (stream state: " + (streams == null ? "NOT_STARTED" : streams.state()) + ")");
        }
        try {
            return streams.store(StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.<String, V>keyValueStore()));
        } catch (InvalidStateStoreException e) {
            throw new RiskDataUnavailableException("Risk data is not available yet: " + e.getMessage());
        }
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/query/PositionView.java`:

```java
package dev.ledgerline.risk.query;

import dev.ledgerline.risk.domain.Position;
import java.math.BigDecimal;

/** A position marked at {@code lastPrice}, the symbol's most recent traded price. */
public record PositionView(
        String symbol,
        long netQuantity,
        BigDecimal averageCost,
        BigDecimal lastPrice,
        BigDecimal notional,
        BigDecimal realizedPnl,
        BigDecimal unrealizedPnl) {

    static PositionView of(Position position, BigDecimal markPrice) {
        return new PositionView(
                position.symbol(),
                position.netQuantity(),
                position.averageCost(),
                markPrice,
                markPrice.multiply(BigDecimal.valueOf(Math.abs(position.netQuantity()))),
                position.realizedPnl(),
                position.unrealizedPnl(markPrice));
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/query/PositionQueryService.java`:

```java
package dev.ledgerline.risk.query;

import dev.ledgerline.risk.domain.Position;
import dev.ledgerline.risk.stream.RiskTopology;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

@Service
public class PositionQueryService {

    private final RiskStores stores;

    public PositionQueryService(RiskStores stores) {
        this.stores = stores;
    }

    public List<PositionView> positionsFor(String accountId) {
        ReadOnlyKeyValueStore<String, String> lastPrices = stores.lastPrices();
        // The trailing "|" keeps "al" from matching "alice|…".
        String prefix = RiskTopology.positionKey(accountId, "");
        List<PositionView> views = new ArrayList<>();
        try (KeyValueIterator<String, Position> entries = stores.positions().prefixScan(prefix, new StringSerializer())) {
            while (entries.hasNext()) {
                KeyValue<String, Position> entry = entries.next();
                views.add(PositionView.of(entry.value, markPrice(entry.value, lastPrices)));
            }
        }
        views.sort(Comparator.comparing(PositionView::symbol));
        return views;
    }

    private static BigDecimal markPrice(Position position, ReadOnlyKeyValueStore<String, String> lastPrices) {
        String lastTraded = lastPrices.get(position.symbol());
        return lastTraded != null ? new BigDecimal(lastTraded) : position.lastPrice();
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/api/PositionController.java`:

```java
package dev.ledgerline.risk.api;

import dev.ledgerline.risk.query.PositionQueryService;
import dev.ledgerline.risk.query.PositionView;
import dev.ledgerline.risk.query.RiskDataUnavailableException;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/risk/accounts")
class PositionController {

    private final PositionQueryService queryService;

    PositionController(PositionQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{accountId}/positions")
    List<PositionView> positions(@PathVariable String accountId) {
        return queryService.positionsFor(accountId);
    }

    @ExceptionHandler(RiskDataUnavailableException.class)
    ProblemDetail handleUnavailable(RiskDataUnavailableException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        problem.setTitle("Risk data unavailable");
        return problem;
    }
}
```

Create `risk-service/src/main/java/dev/ledgerline/risk/stream/RiskStreamsConfiguration.java`:

```java
package dev.ledgerline.risk.stream;

import dev.ledgerline.events.Topics;
import dev.ledgerline.risk.RiskProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
class RiskStreamsConfiguration {

    /** Declared here as well so the source topic exists before the stream starts, even if order-service never ran. */
    @Bean
    NewTopic tradesExecutedTopic() {
        return TopicBuilder.name(Topics.TRADES_EXECUTED).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic riskAlertsTopic() {
        return TopicBuilder.name(Topics.RISK_ALERTS).partitions(3).replicas(1).build();
    }

    @Bean
    RiskTopology riskTopology(StreamsBuilder streamsBuilder, JsonMapper jsonMapper, RiskProperties properties) {
        RiskTopology topology = new RiskTopology(jsonMapper, properties);
        topology.addTo(streamsBuilder);
        return topology;
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./mvnw -B -pl risk-service -am verify`
Expected:
- `PositionQueryServiceTest` passes 3 tests.
- `PositionControllerTest` passes 2 tests.
- `RiskTopologyTest` passes 7 tests and `PositionTest` passes 8.
- `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add risk-service
git commit -m "Serve marked-to-market positions from Kafka Streams state stores"
```

---

### Task 4: End-to-end test, demo, docs and acceptance run

**Files:**
- Create: `risk-service/src/test/java/dev/ledgerline/risk/KafkaTestConfiguration.java`
- Create: `risk-service/src/test/java/dev/ledgerline/risk/RiskServiceIT.java`
- Create: `scripts/demo-risk.sh`
- Modify: `README.md`

**Interfaces:**
- Consumes: the running `risk-service` application (Tasks 1–3); order-service `POST /api/v1/orders`
  (phase 1 and 2A).
- Produces: runnable demo and documentation. No code interfaces.

- [ ] **Step 1: Write the end-to-end test**

Create `risk-service/src/test/java/dev/ledgerline/risk/KafkaTestConfiguration.java`:

```java
package dev.ledgerline.risk;

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

Create `risk-service/src/test/java/dev/ledgerline/risk/RiskServiceIT.java`:

```java
package dev.ledgerline.risk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.events.RiskAlert;
import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Real broker, real Kafka Streams with exactly_once_v2, real REST endpoint. */
@SpringBootTest(properties = {
        "ledgerline.risk.position-notional-limit=1000",
        "spring.kafka.streams.state-dir=${java.io.tmpdir}/risk-service-it-${random.uuid}"
})
@AutoConfigureMockMvc
@Import(KafkaTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class RiskServiceIT {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaContainer kafka;

    @Test
    void tradesBecomePositionsDuplicatesAreIgnoredAndCrossingTheLimitAlertsOnce() throws Exception {
        String run = UUID.randomUUID().toString().substring(0, 8);
        String buyer = "buyer-" + run;
        String seller = "seller-" + run;
        String tradeJson = TopologyFixture.JSON.writeValueAsString(new TradeExecuted("T-" + run, "ACME", "USD",
                new BigDecimal("100.0000"), 11, 1, buyer, 2, seller, "BUY", Instant.parse("2026-09-16T10:00:00Z")));

        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", tradeJson).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", tradeJson).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> mockMvc
                .perform(get("/api/v1/risk/accounts/{account}/positions", buyer))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].symbol").value("ACME"))
                .andExpect(jsonPath("$[0].netQuantity").value(11)));
        mockMvc.perform(get("/api/v1/risk/accounts/{account}/positions", seller))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].netQuantity").value(-11));

        List<RiskAlert> alerts = alertsForRun(run, Duration.ofSeconds(15));
        assertThat(alerts).as("one alert per side, the duplicate adds none")
                .extracting(RiskAlert::accountId)
                .containsExactlyInAnyOrder(buyer, seller);
        assertThat(alerts).allSatisfy(alert -> assertThat(alert.notional()).isEqualByComparingTo("1100"));
    }

    private List<RiskAlert> alertsForRun(String run, Duration window) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "risk-it-" + run,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        List<RiskAlert> alerts = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(Topics.RISK_ALERTS));
            long deadline = System.nanoTime() + window.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    RiskAlert alert = TopologyFixture.JSON.readValue(record.value(), RiskAlert.class);
                    if (alert.tradeId().equals("T-" + run)) {
                        alerts.add(alert);
                    }
                }
            }
        }
        return alerts;
    }
}
```

The consumer polls for the whole window rather than stopping at 2 alerts, so a third (duplicate) alert
would be caught.

- [ ] **Step 2: Run it**

Run: `./mvnw -B -pl risk-service -am verify`
Expected:
- `RiskServiceIT` passes 1 test.
- All unit tests still pass: 8 + 7 + 3 + 2.
- `BUILD SUCCESS`.

If it fails because Kafka Streams can't start `exactly_once_v2` (transaction state log replication), check
the Kafka container's environment. `org.testcontainers.kafka.KafkaContainer` sets the transaction state log
replication factor to 1 for a single broker; if it doesn't, stop and report rather than weakening the
processing guarantee.

- [ ] **Step 3: Write the demo script**

Create `scripts/demo-risk.sh`:

```bash
#!/usr/bin/env bash
# Trades flow into live positions and P&L; a large position raises one limit alert.
# Needs: docker compose stack running, order-service on 8081, risk-service on 8083.
set -euo pipefail

DOCKER="${DOCKER:-docker}"
ORDER_URL="${ORDER_URL:-http://localhost:8081}"
RISK_URL="${RISK_URL:-http://localhost:8083}"
# Not `tr </dev/urandom | head`: under pipefail, tr's SIGPIPE would abort the script.
SYMBOL="$(python3 -c 'import random, string; print("".join(random.choices(string.ascii_uppercase, k=5)))')"
RUN="$(date +%s)"
TRADER="trader-$RUN"
MARKET="market-$RUN"
JSON='Content-Type: application/json'

order() { # account, side, price, quantity
  curl -s -o /dev/null -w "$1 $2 $4 @ $3: HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$JSON" \
    -d "{\"accountId\":\"$1\",\"symbol\":\"$SYMBOL\",\"side\":\"$2\",\"type\":\"LIMIT\",\"price\":$3,\"quantity\":$4}"
}

positions() {
  curl -s "$RISK_URL/api/v1/risk/accounts/$1/positions"; echo
}

echo "== $TRADER buys 100 $SYMBOL at 50, then 100 more at 60 (average cost 55)"
order "$MARKET" SELL 50 100;  order "$TRADER" BUY 50 100
order "$MARKET" SELL 60 100;  order "$TRADER" BUY 60 100
echo "== $TRADER sells 50 at 70 (realised +750)"
order "$MARKET" BUY 70 50;    order "$TRADER" SELL 70 50
sleep 3
echo "== $TRADER positions (net 150, avg 55, realised 750, marked at 70 -> unrealised 2250)"
positions "$TRADER"
echo "== $MARKET positions (short 150)"
positions "$MARKET"

echo "== $TRADER buys 20000 more at 70 (notional 20,150 x 70 = 1,410,500 > 1,000,000 limit)"
order "$MARKET" SELL 70 20000; order "$TRADER" BUY 70 20000
sleep 3
echo "== Risk alerts for $SYMBOL"
"$DOCKER" compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ledgerline.risk.alerts --from-beginning --timeout-ms 5000 --property print.key=true 2>/dev/null \
  | grep "|$SYMBOL" || echo "(no alert found)"
```

Make it executable: `chmod +x scripts/demo-risk.sh`

- [ ] **Step 4: Update the README**

In `README.md`:

1. Under the architecture code block's closing fence, add:

````markdown

```
Kafka: ledgerline.trades.executed ──► risk-service (Kafka Streams, exactly_once_v2)
                                        dedupe by trade id → fills → positions (avg cost, realised P&L)
                                        ├─► last traded price per symbol
                                        ├─► Kafka: ledgerline.risk.alerts (first crossing of the notional limit)
                                        └─► REST: /api/v1/risk/accounts/{id}/positions (marked to market)
```
````

2. In the Modules table, after the `settlement-service` row, add:

```markdown
| `risk-service` | Real-time positions, average-cost realised/unrealised P&L, limit alerts, positions API | Spring Boot 4, Kafka Streams, RocksDB state stores |
```

3. In "Design decisions", after the "Poison messages don't block." bullet, add:

```markdown
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
```

4. In "Failure demos", after the poison-message line inside the code block, add:

```bash
DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker ./scripts/demo-risk.sh             # positions, P&L and a limit alert
```

and change the `./mvnw -pl settlement-service spring-boot:run` line in "Running locally" to be followed by:

```bash
./mvnw -pl risk-service spring-boot:run                # http://localhost:8083
```

5. In "Testing", after the "Database outage" bullet, add:

```markdown
- **Risk:** unit tests for average-cost maths (adding, reducing, flipping, closing, limit crossing); `TopologyTestDriver`
  tests for de-duplication, retention, alerts, last prices and bad input; and an end-to-end test against real
  Kafka with `exactly_once_v2`.
```

6. In "Roadmap", change `  - [ ] C. Real-time risk and P&L with Kafka Streams` to
   `  - [x] C. Real-time risk and P&L with Kafka Streams`.

- [ ] **Step 5: Full build**

Run: `./mvnw -B clean verify`
Expected: `BUILD SUCCESS`, and every test in all six modules passes with none skipped.

- [ ] **Step 6: Manual acceptance run**

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
docker compose up -d kafka kafka-ui postgres
java -Xmx384m -jar order-service/target/order-service-0.1.0-SNAPSHOT.jar &
java -Xmx384m -jar settlement-service/target/settlement-service-0.1.0-SNAPSHOT.jar &
java -Xmx384m -jar risk-service/target/risk-service-0.1.0-SNAPSHOT.jar &
```

Once all three `/actuator/health` endpoints return `UP`, run `DOCKER=… ./scripts/demo-risk.sh` and check:
1. The trader shows net 150, average cost 55, realised P&L 750, last price 70 and unrealised 2250.
2. The market account shows net −150.
3. After the 20,000 buy, exactly one alert for the trader and one for the market account appear for the symbol.
4. `GET /api/v1/risk/accounts/{id}/positions` for an account with no trades returns `[]`.

Stop the services and `docker compose stop` afterwards.

- [ ] **Step 7: Commit**

```bash
git add risk-service/src/test scripts/demo-risk.sh README.md
git commit -m "Add risk-service end-to-end test, demo and docs"
```

Ask the user before pushing.
