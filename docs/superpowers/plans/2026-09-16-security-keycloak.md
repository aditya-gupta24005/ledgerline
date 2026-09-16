# Security with Keycloak Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every Ledgerline API call carries a Keycloak-issued JWT; traders can only trade, cancel and read
for their own account; RISK and OPS can read any account; only OPS can use the dead-letter tools.

**Architecture:**
- **Identity:** Keycloak runs in Docker Compose and imports a `ledgerline` realm with roles TRADER, RISK and
  OPS, five dev users, and two public clients. Access tokens carry `realm_access.roles`, `preferred_username`
  and the audience `ledgerline-api`.
- **Shared module:** `ledgerline-security` is a Spring Boot auto-configuration used by all three services. It
  validates JWTs (issuer and audience), maps realm roles to `ROLE_*` authorities, makes `preferred_username`
  the principal name, turns on method security, and maps denials to a 403 ProblemDetail.
- **Rules:** each controller states its rule with `@PreAuthorize`. The trading account is the token's
  username, never a request field. The matching engine refuses to cancel an order the caller doesn't own.

**Tech Stack:** Spring Boot 4.1.1, Spring Security 7.1.1 (OAuth2 resource server, method security,
`spring-security-test`), Keycloak 26.7.3, Testcontainers 2.0.5 (`GenericContainer` for Keycloak), JUnit 5,
MockMvc.

**Spec:** `docs/superpowers/specs/2026-09-15-phase-2-design.md`, section **B**.

## Global Constraints

- **Repo and build:** `/Users/adityagupta/Developer/ledgerline`, `./mvnw`. No dependency versions outside the
  Spring Boot 4.1.1 BOM.
- **Realm:** `ledgerline`. Issuer `http://localhost:8180/realms/ledgerline`. Audience `ledgerline-api`.
- **Roles:** realm roles `TRADER`, `RISK`, `OPS`, mapped to authorities `ROLE_TRADER`, `ROLE_RISK`, `ROLE_OPS`.
- **Dev users (password = username):** `alice`, `bob`, `carol` (TRADER); `rita` (RISK); `oscar` (OPS).
- **Clients:** `ledgerline-dashboard` (public, authorization code + PKCE S256, redirect
  `http://localhost:5173/*`, web origin `http://localhost:5173`) and `ledgerline-cli` (public, direct access
  grants, dev only). Both add audience `ledgerline-api` to access tokens.
- **Principal:** the account id is the token's `preferred_username` claim (`Authentication.getName()`).
- **Public endpoints:** `/actuator/health`, `/actuator/health/**`, `/actuator/info`. Everything else needs a token.
- **Responses:** no or invalid token → 401 (Spring's bearer-token entry point); wrong role or wrong account
  → 403 ProblemDetail with title `Forbidden`; cancelling another account's order → 404.
- **CORS:** `ledgerline.security.cors.allowed-origins`, default `http://localhost:5173`.
- **Docker CLI:** `/Applications/Docker.app/Contents/Resources/bin/docker` (not on PATH).
- **Commits:** one per task, no attribution lines; `./mvnw -B verify` green first.
- **Deliberate deviations from spec section B:**
  - Audience validation uses Boot's `spring.security.oauth2.resourceserver.jwt.audiences` property, which
    installs the standard audience validator, instead of a hand-written validator class.
  - There is no `CurrentUser` helper: with `preferred_username` as the principal claim, controllers use
    `Authentication.getName()` and SpEL `authentication.name`.
  - Authorization rules live on the controllers as `@PreAuthorize` (method security) rather than one central
    path matrix, so each rule sits next to the endpoint it protects.
  - Demo scripts trade as realm users (alice, bob) because the account now comes from the token.

## File Structure

**New module `ledgerline-security`**

| File | Responsibility |
|---|---|
| `pom.xml` (root) | Adds the module and manages its version |
| `ledgerline-security/pom.xml` | Library POM |
| `…/security/LedgerlineSecurityProperties.java` | `ledgerline.security.cors.allowed-origins` |
| `…/security/RealmRoleConverter.java` | `realm_access.roles` → `ROLE_*` |
| `…/security/SecurityProblemHandler.java` | `AccessDeniedException` → 403 ProblemDetail |
| `…/security/LedgerlineSecurityAutoConfiguration.java` | Filter chain, method security, CORS, converter wiring |
| `src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` | Registers the auto-configuration |
| `src/test/…/security/RealmRoleConverterTest.java` | Converter unit tests |

**matching-engine**

| File | Responsibility |
|---|---|
| `…/matching/OrderBook.java`, `MatchingEngine.java` | Owner-checked cancel |
| `…/matching/MatchingEngineTest.java`, `OrderBookInvariantTest.java` | Updated call sites and new ownership test |

**order-service**

| File | Responsibility |
|---|---|
| `pom.xml`, `application.yml` | Security dependency and JWT properties |
| `…/order/api/PlaceOrderRequest.java` | `accountId` removed; `toEngineRequest(String accountId)` |
| `…/order/api/OrderController.java` | Account from token; TRADER rules |
| `…/order/engine/OrderGateway.java` | `cancel(symbol, orderId, accountId)` |
| `src/test/…/order/TestUsers.java` | `jwt()` helpers |
| `src/test/…/order/api/OrderControllerTest.java`, `EngineHaltedApiTest.java`, `DatabaseOutageHaltIT.java`, `…/engine/OrderGatewayTest.java` | Updated for tokens |
| `src/test/…/order/KeycloakSecurityIT.java` | Real Keycloak end to end |

**settlement-service**

| File | Responsibility |
|---|---|
| `pom.xml`, `application.yml` | Security dependency and JWT properties |
| `…/settlement/api/BalanceController.java` | Own account or RISK/OPS |
| `…/settlement/ops/DeadLetterController.java` | OPS only |
| `src/test/…/settlement/TestUsers.java`, `…/api/BalanceControllerSecurityTest.java`, `…/ops/DeadLetterOpsIT.java` | Tests |

**risk-service**

| File | Responsibility |
|---|---|
| `pom.xml`, `application.yml` | Security dependency and JWT properties |
| `…/risk/api/PositionController.java` | Own account or RISK/OPS |
| `src/test/…/risk/TestUsers.java`, `…/api/PositionControllerTest.java`, `…/RiskServiceIT.java` | Tests |

**Infrastructure, scripts, docs**

| File | Responsibility |
|---|---|
| `keycloak/ledgerline-realm.json` | Realm import |
| `docker-compose.yml` | Keycloak with realm import |
| `scripts/token.sh` | Password-grant token helper for demos |
| `scripts/demo-security.sh` | Role and ownership demo |
| `scripts/demo-kafka-outage.sh`, `demo-poison-message.sh`, `demo-risk.sh` | Updated to send tokens |
| `README.md` | Security section |

---

### Task 1: Shared security auto-configuration

**Files:**
- Modify: `pom.xml` (root)
- Create: `ledgerline-security/pom.xml`
- Create: `ledgerline-security/src/main/java/dev/ledgerline/security/LedgerlineSecurityProperties.java`
- Create: `ledgerline-security/src/main/java/dev/ledgerline/security/RealmRoleConverter.java`
- Create: `ledgerline-security/src/main/java/dev/ledgerline/security/SecurityProblemHandler.java`
- Create: `ledgerline-security/src/main/java/dev/ledgerline/security/LedgerlineSecurityAutoConfiguration.java`
- Create: `ledgerline-security/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `ledgerline-security/src/test/java/dev/ledgerline/security/RealmRoleConverterTest.java`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces:
  - Maven artifact `dev.ledgerline:ledgerline-security` (brings in
    `spring-boot-starter-security-oauth2-resource-server` transitively).
  - `public class LedgerlineSecurityAutoConfiguration` (importable into `@WebMvcTest` slices).
  - `public final class RealmRoleConverter implements Converter<Jwt, Collection<GrantedAuthority>>` with
    `USERNAME_CLAIM = "preferred_username"`.
  - Authorities `ROLE_TRADER`, `ROLE_RISK`, `ROLE_OPS`; principal name = `preferred_username`.
  - 403 responses are `ProblemDetail` with title `Forbidden`.

- [ ] **Step 1: Register the module**

In the root `pom.xml`, add `<module>ledgerline-security</module>` after `<module>ledgerline-events</module>`,
and in `<dependencyManagement>` add after the `matching-engine` entry:

```xml
            <dependency>
                <groupId>dev.ledgerline</groupId>
                <artifactId>ledgerline-security</artifactId>
                <version>${project.version}</version>
            </dependency>
```

Create `ledgerline-security/pom.xml`:

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

    <artifactId>ledgerline-security</artifactId>
    <name>Ledgerline :: Security</name>
    <description>JWT resource-server auto-configuration shared by every service</description>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-security-oauth2-resource-server</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webmvc</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: Write the failing converter test**

Create `ledgerline-security/src/test/java/dev/ledgerline/security/RealmRoleConverterTest.java`:

```java
package dev.ledgerline.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

class RealmRoleConverterTest {

    private final RealmRoleConverter converter = new RealmRoleConverter();

    private static Jwt jwtWithClaim(String name, Object value) {
        return Jwt.withTokenValue("token").header("alg", "none").subject("alice").claim(name, value).build();
    }

    @Test
    void mapsKnownRealmRolesToRoleAuthorities() {
        Jwt jwt = jwtWithClaim("realm_access", Map.of("roles", List.of("TRADER", "OPS", "offline_access")));

        assertThat(converter.convert(jwt))
                .extracting(GrantedAuthority::getAuthority)
                .containsExactlyInAnyOrder("ROLE_TRADER", "ROLE_OPS");
    }

    @Test
    void tokenWithoutRealmAccessHasNoAuthorities() {
        assertThat(converter.convert(jwtWithClaim("scope", "openid"))).isEmpty();
    }

    @Test
    void malformedRolesClaimIsIgnored() {
        assertThat(converter.convert(jwtWithClaim("realm_access", Map.of("roles", "TRADER")))).isEmpty();
    }
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `./mvnw -B -pl ledgerline-security -am test -Dtest=RealmRoleConverterTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation fails with `cannot find symbol … class RealmRoleConverter`.

- [ ] **Step 4: Implement the module**

Create `ledgerline-security/src/main/java/dev/ledgerline/security/LedgerlineSecurityProperties.java`:

```java
package dev.ledgerline.security;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("ledgerline.security")
public record LedgerlineSecurityProperties(@DefaultValue Cors cors) {

    public record Cors(@DefaultValue("http://localhost:5173") List<String> allowedOrigins) {
    }
}
```

Create `ledgerline-security/src/main/java/dev/ledgerline/security/RealmRoleConverter.java`:

```java
package dev.ledgerline.security;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

/** Maps Keycloak's {@code realm_access.roles} to {@code ROLE_*} authorities, keeping only Ledgerline's roles. */
public final class RealmRoleConverter implements Converter<Jwt, Collection<GrantedAuthority>> {

    public static final String USERNAME_CLAIM = "preferred_username";

    private static final Set<String> KNOWN_ROLES = Set.of("TRADER", "RISK", "OPS");

    @Override
    public Collection<GrantedAuthority> convert(Jwt jwt) {
        Map<String, Object> realmAccess = jwt.getClaimAsMap("realm_access");
        if (realmAccess == null || !(realmAccess.get("roles") instanceof Collection<?> roles)) {
            return List.of();
        }
        return roles.stream()
                .map(String::valueOf)
                .filter(KNOWN_ROLES::contains)
                .<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                .toList();
    }
}
```

Create `ledgerline-security/src/main/java/dev/ledgerline/security/SecurityProblemHandler.java`:

```java
package dev.ledgerline.security;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Method-security denials reach the controller layer as {@link AccessDeniedException}; answer with a 403 problem. */
@RestControllerAdvice
public class SecurityProblemHandler {

    @ExceptionHandler(AccessDeniedException.class)
    ProblemDetail handleAccessDenied(AccessDeniedException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.FORBIDDEN, "You do not have permission to perform this action");
        problem.setTitle("Forbidden");
        return problem;
    }
}
```

Create `ledgerline-security/src/main/java/dev/ledgerline/security/LedgerlineSecurityAutoConfiguration.java`:

```java
package dev.ledgerline.security;

import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.security.autoconfigure.actuate.web.servlet.ManagementWebSecurityAutoConfiguration;
import org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration;
import org.springframework.boot.security.oauth2.server.resource.autoconfigure.web.OAuth2ResourceServerWebSecurityAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

/**
 * Stateless JWT resource server for every Ledgerline service.
 *
 * <p>Issuer and audience checks come from the standard Boot properties
 * ({@code spring.security.oauth2.resourceserver.jwt.issuer-uri} and {@code …jwt.audiences}). This class adds
 * the Keycloak role mapping, the username principal, CORS for the dashboard, and method security so each
 * controller can state its own rule with {@code @PreAuthorize}.
 */
@AutoConfiguration(before = {
        ServletWebSecurityAutoConfiguration.class,
        OAuth2ResourceServerWebSecurityAutoConfiguration.class,
        ManagementWebSecurityAutoConfiguration.class})
@EnableWebSecurity
@EnableMethodSecurity
@EnableConfigurationProperties(LedgerlineSecurityProperties.class)
public class LedgerlineSecurityAutoConfiguration {

    @Bean
    SecurityFilterChain ledgerlineSecurityFilterChain(HttpSecurity http, LedgerlineSecurityProperties properties)
            throws Exception {
        JwtAuthenticationConverter jwtConverter = new JwtAuthenticationConverter();
        jwtConverter.setPrincipalClaimName(RealmRoleConverter.USERNAME_CLAIM);
        jwtConverter.setJwtGrantedAuthoritiesConverter(new RealmRoleConverter());

        http
                .csrf(AbstractHttpConfigurer::disable)
                .cors(cors -> cors.configurationSource(corsConfigurationSource(properties)))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(requests -> requests
                        .requestMatchers("/actuator/health", "/actuator/health/**", "/actuator/info").permitAll()
                        .anyRequest().authenticated())
                .oauth2ResourceServer(resourceServer -> resourceServer
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtConverter)));
        return http.build();
    }

    @Bean
    SecurityProblemHandler securityProblemHandler() {
        return new SecurityProblemHandler();
    }

    private static CorsConfigurationSource corsConfigurationSource(LedgerlineSecurityProperties properties) {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(properties.cors().allowedOrigins());
        configuration.setAllowedMethods(List.of("GET", "POST", "DELETE", "OPTIONS"));
        configuration.setAllowedHeaders(List.of("Authorization", "Content-Type"));
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }
}
```

Create `ledgerline-security/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:

```
dev.ledgerline.security.LedgerlineSecurityAutoConfiguration
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./mvnw -B -pl ledgerline-security -am verify`
Expected: `RealmRoleConverterTest` passes 3 tests, `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add pom.xml ledgerline-security
git commit -m "Add shared JWT resource-server auto-configuration with Keycloak role mapping"
```

---

### Task 2: Owner-checked cancel in the matching engine

**Files:**
- Modify: `matching-engine/src/main/java/dev/ledgerline/matching/OrderBook.java:68-79`
- Modify: `matching-engine/src/main/java/dev/ledgerline/matching/MatchingEngine.java:35-38`
- Modify: `matching-engine/src/test/java/dev/ledgerline/matching/MatchingEngineTest.java` (Cancellation tests)
- Modify: `matching-engine/src/test/java/dev/ledgerline/matching/OrderBookInvariantTest.java:40`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `public boolean MatchingEngine.cancel(String symbol, long orderId, String accountId)`. Returns
  `false` (and leaves the book untouched) when the order is unknown, already filled, or owned by another account.

- [ ] **Step 1: Update the tests**

In `MatchingEngineTest.java`, inside `class Cancellation`, change the three existing cancel calls to pass the
owner and add one new test. The class becomes:

```java
    @Nested
    class Cancellation {

        @Test
        void removesOrderFromMiddleOfQueueAndKeepsPriorityOfOthers() {
            MatchResult first = limit("a", SELL, 100_00, 1);
            MatchResult middle = limit("b", SELL, 100_00, 2);
            MatchResult last = limit("c", SELL, 100_00, 3);

            assertThat(engine.cancel(SYMBOL, middle.orderId(), "b")).isTrue();
            assertThat(book().asks()).containsExactly(new LevelView(100_00, 4, 2));

            MatchResult buy = limit("buyer", BUY, 100_00, 4);
            assertThat(buy.trades())
                    .extracting(Trade::sellOrderId)
                    .containsExactly(first.orderId(), last.orderId());
        }

        @Test
        void removesPriceLevelOnceEmpty() {
            MatchResult bid = limit("alice", BUY, 99_00, 1);

            assertThat(engine.cancel(SYMBOL, bid.orderId(), "alice")).isTrue();
            assertThat(engine.bestBid(SYMBOL)).isEmpty();
        }

        @Test
        void returnsFalseForFilledOrUnknownOrders() {
            MatchResult sell = limit("seller", SELL, 100_00, 1);
            limit("buyer", BUY, 100_00, 1);

            assertThat(engine.cancel(SYMBOL, sell.orderId(), "seller")).isFalse();
            assertThat(engine.cancel(SYMBOL, 999, "seller")).isFalse();
            assertThat(engine.cancel("UNKNOWN", 1, "seller")).isFalse();
        }

        @Test
        void anotherAccountCannotCancelTheOrderAndItStaysOnTheBook() {
            MatchResult bid = limit("alice", BUY, 99_00, 7);

            assertThat(engine.cancel(SYMBOL, bid.orderId(), "mallory")).isFalse();

            assertThat(book().bids()).containsExactly(new LevelView(99_00, 7, 1));
            assertThat(engine.cancel(SYMBOL, bid.orderId(), "alice")).isTrue();
        }
    }
```

In `OrderBookInvariantTest.java` line 40, change

```java
                assertThat(engine.cancel(SYMBOL, orderId)).isEqualTo(shouldBeResting);
```

to

```java
                assertThat(engine.cancel(SYMBOL, orderId, "acct")).isEqualTo(shouldBeResting);
```

(every random order in that test is submitted for account `"acct"`).

- [ ] **Step 2: Run to verify they fail**

Run: `./mvnw -B -pl matching-engine test`
Expected: compilation fails, `method cancel in class MatchingEngine cannot be applied to given types`.

- [ ] **Step 3: Implement**

In `OrderBook.java`, replace the `cancel` method with:

```java
    boolean cancel(long orderId, String accountId) {
        Order order = restingOrders.get(orderId);
        if (order == null || !order.accountId.equals(accountId)) {
            return false;
        }
        restingOrders.remove(orderId);
        PriceLevel level = order.level;
        level.remove(order);
        if (level.isEmpty()) {
            levelsFor(order.side).remove(level.priceTicks);
        }
        return true;
    }
```

In `MatchingEngine.java`, replace the `cancel` method with:

```java
    /** Cancels a resting order. Returns false if it is unknown, no longer resting, or owned by another account. */
    public boolean cancel(String symbol, long orderId, String accountId) {
        OrderBook book = books.get(symbol);
        return book != null && book.cancel(orderId, accountId);
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `./mvnw -B -pl matching-engine verify`
Expected: 23 tests pass (22 before plus the new one), `BUILD SUCCESS`.

Note: `order-service` no longer compiles until Task 3; that's expected. Don't run a full build here.

- [ ] **Step 5: Commit**

```bash
git add matching-engine
git commit -m "Refuse to cancel an order that belongs to another account"
```

---

### Task 3: Secure order-service

**Files:**
- Modify: `order-service/pom.xml`
- Modify: `order-service/src/main/resources/application.yml`
- Modify: `order-service/src/main/java/dev/ledgerline/order/api/PlaceOrderRequest.java`
- Modify: `order-service/src/main/java/dev/ledgerline/order/api/OrderController.java`
- Modify: `order-service/src/main/java/dev/ledgerline/order/engine/OrderGateway.java:77-82`
- Create: `order-service/src/test/java/dev/ledgerline/order/TestUsers.java`
- Modify: `order-service/src/test/java/dev/ledgerline/order/engine/OrderGatewayTest.java:74`
- Modify: `order-service/src/test/java/dev/ledgerline/order/api/OrderControllerTest.java`
- Modify: `order-service/src/test/java/dev/ledgerline/order/api/EngineHaltedApiTest.java`
- Modify: `order-service/src/test/java/dev/ledgerline/order/api/DatabaseOutageHaltIT.java`

**Interfaces:**
- Consumes: `ledgerline-security` (Task 1); `MatchingEngine.cancel(symbol, orderId, accountId)` (Task 2).
- Produces:
  - `POST /api/v1/orders` body without `accountId`; account = token username; requires `ROLE_TRADER`.
  - `DELETE /api/v1/orders/{symbol}/{orderId}` requires `ROLE_TRADER`; 404 unless the caller owns the order.
  - `GET /api/v1/books/{symbol}` requires any valid token.
  - `OrderGateway.cancel(String symbol, long orderId, String accountId)`.
  - Test helper `TestUsers.as(String username, String... roles)`, `TestUsers.trader(String)`, `TestUsers.ops()`,
    `TestUsers.risk()` returning `RequestPostProcessor`.

- [ ] **Step 1: Dependencies and properties**

In `order-service/pom.xml`, after the `matching-engine` dependency add:

```xml
        <dependency>
            <groupId>dev.ledgerline</groupId>
            <artifactId>ledgerline-security</artifactId>
        </dependency>
```

and after `spring-boot-starter-webmvc-test` (test scope) add:

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-security-test</artifactId>
            <scope>test</scope>
        </dependency>
```

In `order-service/src/main/resources/application.yml`, inside `spring:` add (after the `flyway:` block):

```yaml
  security:
    oauth2:
      resourceserver:
        jwt:
          issuer-uri: http://localhost:8180/realms/ledgerline
          audiences: ledgerline-api
```

and under the top-level `ledgerline:` block add:

```yaml
  security:
    cors:
      allowed-origins: http://localhost:5173
```

- [ ] **Step 2: Test helper and updated tests**

Create `order-service/src/test/java/dev/ledgerline/order/TestUsers.java`:

```java
package dev.ledgerline.order;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

import java.util.Arrays;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/** Fake bearer tokens for MockMvc: no Keycloak needed, the JWT decoder is never called. */
public final class TestUsers {

    private TestUsers() {
    }

    public static RequestPostProcessor as(String username, String... roles) {
        return jwt()
                .jwt(jwt -> jwt.subject(username).claim("preferred_username", username))
                .authorities(Arrays.stream(roles)
                        .<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                        .toList());
    }

    public static RequestPostProcessor trader(String username) {
        return as(username, "TRADER");
    }

    public static RequestPostProcessor risk() {
        return as("rita", "RISK");
    }

    public static RequestPostProcessor ops() {
        return as("oscar", "OPS");
    }
}
```

Replace `order-service/src/test/java/dev/ledgerline/order/api/OrderControllerTest.java` with:

```java
package dev.ledgerline.order.api;

import static dev.ledgerline.order.TestUsers.ops;
import static dev.ledgerline.order.TestUsers.trader;
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
import org.springframework.test.web.servlet.request.RequestPostProcessor;
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

    private ResultActions placeOrder(RequestPostProcessor user, String json) throws Exception {
        return mockMvc.perform(post("/api/v1/orders").with(user)
                .contentType(MediaType.APPLICATION_JSON).content(json));
    }

    @Test
    void limitOrderOnEmptyBookRests() throws Exception {
        placeOrder(trader("alice"), """
                {"symbol": "RESTS", "side": "BUY", "type": "LIMIT", "price": 100.25, "quantity": 10}
                """)
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("NEW"))
                .andExpect(jsonPath("$.restingQuantity").value(10))
                .andExpect(jsonPath("$.fills").isEmpty());

        verify(outboxWriter, never()).append(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void crossingOrdersTradeUnderTheTokenAccountsAndAppendToTheOutbox() throws Exception {
        placeOrder(trader("alice"), """
                {"symbol": "CROSS", "side": "SELL", "type": "LIMIT", "price": 101.50, "quantity": 5}
                """)
                .andExpect(status().isCreated());

        placeOrder(trader("bob"), """
                {"symbol": "CROSS", "side": "BUY", "type": "LIMIT", "price": 102, "quantity": 5}
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
    void accountIdInTheBodyIsIgnoredInFavourOfTheToken() throws Exception {
        placeOrder(trader("alice"), """
                {"accountId": "mallory", "symbol": "SPOOF", "side": "SELL", "type": "LIMIT", "price": 9, "quantity": 1}
                """)
                .andExpect(status().isCreated());

        mockMvc.perform(delete("/api/v1/orders/SPOOF/{orderId}", 1).with(trader("mallory")))
                .andExpect(status().isNotFound());
    }

    @Test
    void rejectsLimitOrderWithoutPrice() throws Exception {
        placeOrder(trader("alice"), """
                {"symbol": "NOPX", "side": "BUY", "type": "LIMIT", "quantity": 10}
                """)
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsMarketOrderWithPrice() throws Exception {
        placeOrder(trader("alice"), """
                {"symbol": "MKTPX", "side": "BUY", "type": "MARKET", "price": 10, "quantity": 10}
                """)
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsPriceFinerThanOneTick() throws Exception {
        placeOrder(trader("alice"), """
                {"symbol": "TICK", "side": "BUY", "type": "LIMIT", "price": 10.12345, "quantity": 10}
                """)
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Invalid price"));
    }

    @Test
    void onlyTheOwnerCanCancelAndOnlyOnce() throws Exception {
        String body = placeOrder(trader("alice"), """
                {"symbol": "CANCEL", "side": "SELL", "type": "LIMIT", "price": 50, "quantity": 1}
                """)
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long orderId = JsonPath.<Number>read(body, "$.orderId").longValue();

        mockMvc.perform(delete("/api/v1/orders/CANCEL/{orderId}", orderId).with(trader("bob")))
                .andExpect(status().isNotFound());
        mockMvc.perform(delete("/api/v1/orders/CANCEL/{orderId}", orderId).with(trader("alice")))
                .andExpect(status().isNoContent());
        mockMvc.perform(delete("/api/v1/orders/CANCEL/{orderId}", orderId).with(trader("alice")))
                .andExpect(status().isNotFound());
    }

    @Test
    void bookAggregatesDepthPerPriceLevelForAnyAuthenticatedUser() throws Exception {
        placeOrder(trader("alice"), """
                {"symbol": "DEPTH", "side": "BUY", "type": "LIMIT", "price": 99, "quantity": 3}
                """);
        placeOrder(trader("bob"), """
                {"symbol": "DEPTH", "side": "BUY", "type": "LIMIT", "price": 99, "quantity": 4}
                """);
        placeOrder(trader("carol"), """
                {"symbol": "DEPTH", "side": "SELL", "type": "LIMIT", "price": 101, "quantity": 2}
                """);

        mockMvc.perform(get("/api/v1/books/DEPTH").with(ops()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bids.length()").value(1))
                .andExpect(jsonPath("$.bids[0].quantity").value(7))
                .andExpect(jsonPath("$.bids[0].orders").value(2))
                .andExpect(jsonPath("$.asks[0].quantity").value(2));
    }

    @Test
    void requestsWithoutATokenAreUnauthorized() throws Exception {
        mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"symbol": "NOTOK", "side": "BUY", "type": "LIMIT", "price": 1, "quantity": 1}
                        """))
                .andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/books/NOTOK")).andExpect(status().isUnauthorized());
        mockMvc.perform(delete("/api/v1/orders/NOTOK/1")).andExpect(status().isUnauthorized());
    }

    @Test
    void nonTradersCannotPlaceOrCancelOrders() throws Exception {
        placeOrder(ops(), """
                {"symbol": "NOROLE", "side": "BUY", "type": "LIMIT", "price": 1, "quantity": 1}
                """)
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.title").value("Forbidden"));
        mockMvc.perform(delete("/api/v1/orders/NOROLE/1").with(ops())).andExpect(status().isForbidden());
    }

    @Test
    void healthStaysPublic() throws Exception {
        mockMvc.perform(get("/actuator/health")).andExpect(status().isOk());
    }
}
```

In `EngineHaltedApiTest.java`, add `import static dev.ledgerline.order.TestUsers.trader;` and change the
three `post(...)` calls so each reads `post("/api/v1/orders").with(trader("alice"))` (the second with
`trader("bob")`, the third with `trader("carol")`), and remove the `"accountId": "…",` field from each JSON
body. The `get("/api/v1/books/HALT")` call becomes `get("/api/v1/books/HALT").with(trader("alice"))`. The
`/actuator/health` call stays token-less.

In `DatabaseOutageHaltIT.java`, add the same import, change `placeOrder` to:

```java
    private ResultActions placeOrder(String account, String side) throws Exception {
        return mockMvc.perform(post("/api/v1/orders").with(trader(account))
                .contentType(MediaType.APPLICATION_JSON).content("""
                {"symbol": "DBOUT", "side": "%s", "type": "LIMIT", "price": 20, "quantity": 1}
                """.formatted(side)));
    }
```

and change the book read to `mockMvc.perform(get("/api/v1/books/DBOUT").with(trader("outage-seller")))`.

In `OrderGatewayTest.java` line 74, change `gateway.cancel("ACME", 1)` to `gateway.cancel("ACME", 1, "alice")`.

- [ ] **Step 3: Run to verify they fail**

Run: `./mvnw -B -q -pl ledgerline-security,matching-engine install -DskipTests && ./mvnw -B -pl order-service test-compile`
Expected: compilation fails in `OrderGateway`/`OrderController` (`cancel` arity) and in the tests
(`toEngineRequest`, `TestUsers` fine, `cancel` arity in `OrderGatewayTest`).

- [ ] **Step 4: Implement**

Replace `order-service/src/main/java/dev/ledgerline/order/api/PlaceOrderRequest.java` with:

```java
package dev.ledgerline.order.api;

import dev.ledgerline.matching.OrderRequest;
import dev.ledgerline.matching.OrderType;
import dev.ledgerline.matching.Side;
import dev.ledgerline.order.engine.Prices;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

/** The trading account is never part of the request: it is the authenticated user's username. */
public record PlaceOrderRequest(
        @NotBlank @Pattern(regexp = "[A-Z]{1,10}", message = "must be 1-10 uppercase letters") String symbol,
        @NotNull Side side,
        @NotNull OrderType type,
        @Positive BigDecimal price,
        @Positive long quantity) {

    @AssertTrue(message = "price is required for LIMIT orders and must be omitted for MARKET orders")
    public boolean isPriceConsistentWithType() {
        return type == null || (type == OrderType.LIMIT) == (price != null);
    }

    OrderRequest toEngineRequest(String accountId) {
        return type == OrderType.MARKET
                ? OrderRequest.market(accountId, symbol, side, quantity)
                : OrderRequest.limit(accountId, symbol, side, Prices.toTicks(price), quantity);
    }
}
```

Replace `order-service/src/main/java/dev/ledgerline/order/api/OrderController.java` with:

```java
package dev.ledgerline.order.api;

import dev.ledgerline.matching.MatchResult;
import dev.ledgerline.matching.OrderStatus;
import dev.ledgerline.order.engine.OrderGateway;
import dev.ledgerline.order.engine.OrderOutcome;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
class OrderController {

    private static final int MAX_DEPTH = 50;

    private final OrderGateway gateway;

    OrderController(OrderGateway gateway) {
        this.gateway = gateway;
    }

    @PostMapping("/orders")
    @PreAuthorize("hasRole('TRADER')")
    public ResponseEntity<OrderResponse> placeOrder(
            @Valid @RequestBody PlaceOrderRequest request, Authentication authentication) {
        OrderOutcome outcome = gateway.submit(request.toEngineRequest(authentication.getName()));
        MatchResult result = outcome.result();
        if (result.status() == OrderStatus.REJECTED) {
            throw new OrderRejectedException(result.orderId(), result.rejectReason());
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderResponse.from(outcome));
    }

    /** 404 for someone else's order as well as for an unknown one, so callers can't probe other accounts. */
    @DeleteMapping("/orders/{symbol}/{orderId}")
    @PreAuthorize("hasRole('TRADER')")
    public ResponseEntity<Void> cancelOrder(
            @PathVariable String symbol, @PathVariable long orderId, Authentication authentication) {
        return gateway.cancel(symbol, orderId, authentication.getName())
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    @GetMapping("/books/{symbol}")
    public BookResponse book(@PathVariable String symbol, @RequestParam(defaultValue = "10") int depth) {
        return BookResponse.from(gateway.book(symbol, Math.clamp(depth, 1, MAX_DEPTH)));
    }
}
```

In `OrderGateway.java`, replace the `cancel` method with:

```java
    public boolean cancel(String symbol, long orderId, String accountId) {
        return onSequencer(() -> {
            ensureRunning();
            return engine.cancel(symbol, orderId, accountId);
        });
    }
```

- [ ] **Step 5: Run to verify they pass**

Run: `./mvnw -B -pl order-service verify`
Expected:
- `OrderControllerTest` passes 11 tests.
- `EngineHaltedApiTest` 1, `DatabaseOutageHaltIT` 1, `OrderGatewayTest` 3, outbox ITs 7, all pass.
- `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add order-service
git commit -m "Secure order-service: account from the JWT, TRADER role, owner-only cancel"
```

### Task 4: Secure settlement-service and risk-service

**Files:**
- Modify: `settlement-service/pom.xml`, `settlement-service/src/main/resources/application.yml`
- Modify: `settlement-service/src/main/java/dev/ledgerline/settlement/api/BalanceController.java`
- Modify: `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterController.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/TestUsers.java`
- Create: `settlement-service/src/test/java/dev/ledgerline/settlement/api/BalanceControllerSecurityTest.java`
- Modify: `settlement-service/src/test/java/dev/ledgerline/settlement/ops/DeadLetterOpsIT.java`
- Modify: `risk-service/pom.xml`, `risk-service/src/main/resources/application.yml`
- Modify: `risk-service/src/main/java/dev/ledgerline/risk/api/PositionController.java`
- Create: `risk-service/src/test/java/dev/ledgerline/risk/TestUsers.java`
- Modify: `risk-service/src/test/java/dev/ledgerline/risk/api/PositionControllerTest.java`
- Modify: `risk-service/src/test/java/dev/ledgerline/risk/RiskServiceIT.java`

**Interfaces:**
- Consumes: `ledgerline-security` and `LedgerlineSecurityAutoConfiguration` (Task 1).
- Produces:
  - `GET /api/v1/accounts/{accountId}/balances`: RISK or OPS for any account; TRADER only for their own.
  - `GET` and `POST` under `/api/v1/ops/dead-letters`: OPS only.
  - `GET /api/v1/risk/accounts/{accountId}/positions`: RISK or OPS for any account; TRADER only for their own.

- [ ] **Step 1: Dependencies and properties (both services)**

In `settlement-service/pom.xml` and `risk-service/pom.xml`, after the `ledgerline-events` dependency add:

```xml
        <dependency>
            <groupId>dev.ledgerline</groupId>
            <artifactId>ledgerline-security</artifactId>
        </dependency>
```

and among the test dependencies add:

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-security-test</artifactId>
            <scope>test</scope>
        </dependency>
```

In both `application.yml` files, inside `spring:` add:

```yaml
  security:
    oauth2:
      resourceserver:
        jwt:
          issuer-uri: http://localhost:8180/realms/ledgerline
          audiences: ledgerline-api
```

and add a `security` block under the existing top-level `ledgerline:` key:

```yaml
  security:
    cors:
      allowed-origins: http://localhost:5173
```

- [ ] **Step 2: Test helpers**

Create `settlement-service/src/test/java/dev/ledgerline/settlement/TestUsers.java` and
`risk-service/src/test/java/dev/ledgerline/risk/TestUsers.java`. They are identical except for the package
line (`dev.ledgerline.settlement` and `dev.ledgerline.risk`):

```java
package dev.ledgerline.settlement;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

import java.util.Arrays;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/** Fake bearer tokens for MockMvc: no Keycloak needed, the JWT decoder is never called. */
public final class TestUsers {

    private TestUsers() {
    }

    public static RequestPostProcessor as(String username, String... roles) {
        return jwt()
                .jwt(jwt -> jwt.subject(username).claim("preferred_username", username))
                .authorities(Arrays.stream(roles)
                        .<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                        .toList());
    }

    public static RequestPostProcessor trader(String username) {
        return as(username, "TRADER");
    }

    public static RequestPostProcessor risk() {
        return as("rita", "RISK");
    }

    public static RequestPostProcessor ops() {
        return as("oscar", "OPS");
    }
}
```

- [ ] **Step 3: Write the failing tests**

Create `settlement-service/src/test/java/dev/ledgerline/settlement/api/BalanceControllerSecurityTest.java`:

```java
package dev.ledgerline.settlement.api;

import static dev.ledgerline.settlement.TestUsers.ops;
import static dev.ledgerline.settlement.TestUsers.risk;
import static dev.ledgerline.settlement.TestUsers.trader;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.security.LedgerlineSecurityAutoConfiguration;
import dev.ledgerline.settlement.persistence.AssetBalance;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(BalanceController.class)
@Import(LedgerlineSecurityAutoConfiguration.class)
class BalanceControllerSecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JournalEntryRepository journal;

    @BeforeEach
    void bobHasABalance() {
        when(journal.balancesFor("bob")).thenReturn(List.of(new AssetBalance("USD", new BigDecimal("10.0000"))));
    }

    @Test
    void aTraderCanReadTheirOwnBalances() throws Exception {
        mockMvc.perform(get("/api/v1/accounts/bob/balances").with(trader("bob")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].asset").value("USD"));
    }

    @Test
    void aTraderCannotReadAnotherAccount() throws Exception {
        mockMvc.perform(get("/api/v1/accounts/bob/balances").with(trader("alice")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.title").value("Forbidden"));
    }

    @Test
    void riskAndOpsCanReadAnyAccount() throws Exception {
        mockMvc.perform(get("/api/v1/accounts/bob/balances").with(risk())).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/accounts/bob/balances").with(ops())).andExpect(status().isOk());
    }

    @Test
    void noTokenIsUnauthorized() throws Exception {
        mockMvc.perform(get("/api/v1/accounts/bob/balances")).andExpect(status().isUnauthorized());
    }
}
```

In `DeadLetterOpsIT.java`:
- add `import static dev.ledgerline.settlement.TestUsers.ops;` and
  `import static dev.ledgerline.settlement.TestUsers.trader;`;
- append `.with(ops())` to every existing `get(...)` and `post(...)` builder (lines 69, 85, 94, 102, 107), for
  example `get("/api/v1/ops/dead-letters?limit=500").with(ops())` and
  `post("/api/v1/ops/dead-letters/0/{offset}/replay", offset).with(ops())`;
- add two tests at the end of the class:

```java
    @Test
    void tradersAndRiskCannotUseTheOpsApi() throws Exception {
        mockMvc.perform(get("/api/v1/ops/dead-letters").with(trader("alice")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.title").value("Forbidden"));
        mockMvc.perform(post("/api/v1/ops/dead-letters/0/0/replay").with(trader("alice")))
                .andExpect(status().isForbidden());
    }

    @Test
    void noTokenIsUnauthorized() throws Exception {
        mockMvc.perform(get("/api/v1/ops/dead-letters")).andExpect(status().isUnauthorized());
    }
```

Replace `risk-service/src/test/java/dev/ledgerline/risk/api/PositionControllerTest.java` with:

```java
package dev.ledgerline.risk.api;

import static dev.ledgerline.risk.TestUsers.ops;
import static dev.ledgerline.risk.TestUsers.risk;
import static dev.ledgerline.risk.TestUsers.trader;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.risk.query.PositionQueryService;
import dev.ledgerline.risk.query.PositionView;
import dev.ledgerline.risk.query.RiskDataUnavailableException;
import dev.ledgerline.security.LedgerlineSecurityAutoConfiguration;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PositionController.class)
@Import(LedgerlineSecurityAutoConfiguration.class)
class PositionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private PositionQueryService queryService;

    private void aliceHasAPosition() {
        when(queryService.positionsFor("alice")).thenReturn(List.of(new PositionView("ACME", 10,
                new BigDecimal("100.0000"), new BigDecimal("110.0000"), new BigDecimal("1100.0000"),
                new BigDecimal("0.0000"), new BigDecimal("100.0000"))));
    }

    @Test
    void returnsTheCallersOwnPositions() throws Exception {
        aliceHasAPosition();

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(trader("alice")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].symbol").value("ACME"))
                .andExpect(jsonPath("$[0].netQuantity").value(10))
                .andExpect(jsonPath("$[0].averageCost").value(100.0))
                .andExpect(jsonPath("$[0].lastPrice").value(110.0))
                .andExpect(jsonPath("$[0].notional").value(1100.0))
                .andExpect(jsonPath("$[0].unrealizedPnl").value(100.0));
    }

    @Test
    void riskAndOpsCanReadAnyAccount() throws Exception {
        aliceHasAPosition();

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(risk())).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(ops())).andExpect(status().isOk());
    }

    @Test
    void aTraderCannotReadAnotherAccount() throws Exception {
        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(trader("bob")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.title").value("Forbidden"));
    }

    @Test
    void noTokenIsUnauthorized() throws Exception {
        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions")).andExpect(status().isUnauthorized());
    }

    @Test
    void returns503WhileRiskDataIsNotQueryable() throws Exception {
        when(queryService.positionsFor("alice")).thenThrow(new RiskDataUnavailableException("stream state: REBALANCING"));

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(trader("alice")))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("Risk data unavailable"));
    }
}
```

In `RiskServiceIT.java`, add `import static dev.ledgerline.risk.TestUsers.trader;` and change the two
`get(...)` builders to `get("/api/v1/risk/accounts/{account}/positions", buyer).with(trader(buyer))` and
`get("/api/v1/risk/accounts/{account}/positions", seller).with(trader(seller))`.

- [ ] **Step 4: Run to verify they fail**

Run: `./mvnw -B -pl settlement-service,risk-service test -Dtest='BalanceControllerSecurityTest,PositionControllerTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `aTraderCannotReadAnotherAccount` fails in both classes with `Status expected:<403> but was:<200>`
(the filter chain authenticates, but no rule limits accounts yet). If `Import` of `LedgerlineSecurityAutoConfiguration`
is not resolved, Task 1 was not installed: run `./mvnw -B -q -pl ledgerline-security install -DskipTests`.

- [ ] **Step 5: Implement the rules**

Replace `settlement-service/src/main/java/dev/ledgerline/settlement/api/BalanceController.java` with:

```java
package dev.ledgerline.settlement.api;

import dev.ledgerline.settlement.persistence.AssetBalance;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/accounts")
class BalanceController {

    private final JournalEntryRepository journal;

    BalanceController(JournalEntryRepository journal) {
        this.journal = journal;
    }

    /** Risk and ops see every account; a trader only their own (the token's username). */
    @GetMapping("/{accountId}/balances")
    @PreAuthorize("hasAnyRole('RISK', 'OPS') or #accountId == authentication.name")
    public List<AssetBalance> balances(@PathVariable String accountId) {
        return journal.balancesFor(accountId);
    }
}
```

In `settlement-service/src/main/java/dev/ledgerline/settlement/ops/DeadLetterController.java`:
- add `import org.springframework.security.access.prepost.PreAuthorize;`
- add `@PreAuthorize("hasRole('OPS')")` on the line after `@RequestMapping("/api/v1/ops/dead-letters")`
- make `list(...)` and `replay(...)` `public` (method security proxies need overridable public methods).

Replace `risk-service/src/main/java/dev/ledgerline/risk/api/PositionController.java` with:

```java
package dev.ledgerline.risk.api;

import dev.ledgerline.risk.query.PositionQueryService;
import dev.ledgerline.risk.query.PositionView;
import dev.ledgerline.risk.query.RiskDataUnavailableException;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.security.access.prepost.PreAuthorize;
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

    /** Risk and ops see every account; a trader only their own (the token's username). */
    @GetMapping("/{accountId}/positions")
    @PreAuthorize("hasAnyRole('RISK', 'OPS') or #accountId == authentication.name")
    public List<PositionView> positions(@PathVariable String accountId) {
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

- [ ] **Step 6: Run to verify they pass**

Run: `./mvnw -B -pl settlement-service,risk-service verify`
Expected:
- settlement: `BalanceControllerSecurityTest` 4, `DeadLetterOpsIT` 6, plus every earlier test, all pass.
- risk: `PositionControllerTest` 5, `RiskServiceIT` 1, plus every earlier test, all pass.
- `BUILD SUCCESS`.

- [ ] **Step 7: Commit**

```bash
git add settlement-service risk-service
git commit -m "Secure settlement and risk APIs: own-account reads for traders, OPS-only dead letters"
```

---

### Task 5: Keycloak realm, Compose, and a real-token end-to-end test

**Files:**
- Create: `keycloak/ledgerline-realm.json`
- Modify: `docker-compose.yml` (keycloak service)
- Create: `order-service/src/test/java/dev/ledgerline/order/KeycloakSecurityIT.java`

**Interfaces:**
- Consumes: the secured order-service (Task 3).
- Produces: a realm file that both Compose and the test import; Keycloak reachable at `http://localhost:8180`
  with the users, roles and clients listed in Global Constraints.

- [ ] **Step 1: Realm import file**

Create `keycloak/ledgerline-realm.json`:

```json
{
  "realm": "ledgerline",
  "enabled": true,
  "accessTokenLifespan": 3600,
  "roles": {
    "realm": [
      { "name": "TRADER", "description": "Places and cancels own orders; reads own balances and positions" },
      { "name": "RISK",   "description": "Reads every account's balances and positions" },
      { "name": "OPS",    "description": "Operates the platform, including dead-letter replay" }
    ]
  },
  "users": [
    { "username": "alice", "enabled": true, "email": "alice@ledgerline.dev", "emailVerified": true,
      "credentials": [{ "type": "password", "value": "alice", "temporary": false }], "realmRoles": ["TRADER"] },
    { "username": "bob",   "enabled": true, "email": "bob@ledgerline.dev", "emailVerified": true,
      "credentials": [{ "type": "password", "value": "bob",   "temporary": false }], "realmRoles": ["TRADER"] },
    { "username": "carol", "enabled": true, "email": "carol@ledgerline.dev", "emailVerified": true,
      "credentials": [{ "type": "password", "value": "carol", "temporary": false }], "realmRoles": ["TRADER"] },
    { "username": "rita",  "enabled": true, "email": "rita@ledgerline.dev", "emailVerified": true,
      "credentials": [{ "type": "password", "value": "rita",  "temporary": false }], "realmRoles": ["RISK"] },
    { "username": "oscar", "enabled": true, "email": "oscar@ledgerline.dev", "emailVerified": true,
      "credentials": [{ "type": "password", "value": "oscar", "temporary": false }], "realmRoles": ["OPS"] }
  ],
  "clients": [
    {
      "clientId": "ledgerline-dashboard",
      "name": "Ledgerline dashboard (browser, PKCE)",
      "enabled": true,
      "protocol": "openid-connect",
      "publicClient": true,
      "standardFlowEnabled": true,
      "directAccessGrantsEnabled": false,
      "redirectUris": ["http://localhost:5173/*"],
      "webOrigins": ["http://localhost:5173"],
      "attributes": { "pkce.code.challenge.method": "S256", "post.logout.redirect.uris": "http://localhost:5173/*" },
      "protocolMappers": [
        {
          "name": "ledgerline-api audience",
          "protocol": "openid-connect",
          "protocolMapper": "oidc-audience-mapper",
          "config": {
            "included.custom.audience": "ledgerline-api",
            "access.token.claim": "true",
            "id.token.claim": "false",
            "introspection.token.claim": "true"
          }
        }
      ]
    },
    {
      "clientId": "ledgerline-cli",
      "name": "Ledgerline CLI (dev only: password grant for curl demos)",
      "enabled": true,
      "protocol": "openid-connect",
      "publicClient": true,
      "standardFlowEnabled": false,
      "directAccessGrantsEnabled": true,
      "protocolMappers": [
        {
          "name": "ledgerline-api audience",
          "protocol": "openid-connect",
          "protocolMapper": "oidc-audience-mapper",
          "config": {
            "included.custom.audience": "ledgerline-api",
            "access.token.claim": "true",
            "id.token.claim": "false",
            "introspection.token.claim": "true"
          }
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Compose**

In `docker-compose.yml`, replace the `keycloak:` service with:

```yaml
  # Identity provider. Imports keycloak/ledgerline-realm.json on start. Admin console: http://localhost:8180
  keycloak:
    image: quay.io/keycloak/keycloak:26.7.3
    command: start-dev --import-realm
    ports:
      - "8180:8080"
    environment:
      KC_BOOTSTRAP_ADMIN_USERNAME: admin
      KC_BOOTSTRAP_ADMIN_PASSWORD: admin
    volumes:
      - ./keycloak:/opt/keycloak/data/import:ro
```

- [ ] **Step 3: Write the end-to-end test**

Create `order-service/src/test/java/dev/ledgerline/order/KeycloakSecurityIT.java`:

```java
package dev.ledgerline.order;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import dev.ledgerline.order.outbox.OutboxWriter;

/**
 * Real Keycloak, real tokens, real JWT validation: issuer, signature, audience and role mapping all
 * exercised end to end. The same realm file Compose imports is mounted into the container.
 */
@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@AutoConfigureMockMvc
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class KeycloakSecurityIT {

    @Container
    static GenericContainer<?> keycloak = new GenericContainer<>("quay.io/keycloak/keycloak:26.7.3")
            .withCommand("start-dev", "--import-realm")
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("..", "keycloak", "ledgerline-realm.json")),
                    "/opt/keycloak/data/import/ledgerline-realm.json")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/realms/ledgerline").forPort(8080).forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    @DynamicPropertySource
    static void issuer(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", KeycloakSecurityIT::realmUrl);
    }

    static String realmUrl() {
        return "http://localhost:" + keycloak.getMappedPort(8080) + "/realms/ledgerline";
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OutboxWriter outboxWriter;

    private static String bearer(String username) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "password");
        form.add("client_id", "ledgerline-cli");
        form.add("username", username);
        form.add("password", username);
        Map<?, ?> response = RestClient.create()
                .post()
                .uri(realmUrl() + "/protocol/openid-connect/token")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(form)
                .retrieve()
                .body(Map.class);
        return "Bearer " + response.get("access_token");
    }

    @Test
    void aTraderTokenFromKeycloakPlacesAnOrderUnderTheirUsername() throws Exception {
        mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, bearer("alice"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"symbol": "KCLK", "side": "SELL", "type": "LIMIT", "price": 12.5, "quantity": 3}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("NEW"));

        mockMvc.perform(get("/api/v1/books/KCLK").header(HttpHeaders.AUTHORIZATION, bearer("rita")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.asks[0].quantity").value(3));
    }

    @Test
    void anOpsTokenCannotTrade() throws Exception {
        mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, bearer("oscar"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"symbol": "KCLK", "side": "BUY", "type": "LIMIT", "price": 12.5, "quantity": 1}
                                """))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.title").value("Forbidden"));
    }

    @Test
    void aTamperedTokenIsRejected() throws Exception {
        String token = bearer("alice");
        String tampered = token.substring(0, token.length() - 4) + "AAAA";

        mockMvc.perform(get("/api/v1/books/KCLK").header(HttpHeaders.AUTHORIZATION, tampered))
                .andExpect(status().isUnauthorized());
    }
}
```

- [ ] **Step 4: Run it**

Run: `./mvnw -B -pl order-service verify -Dit.test=KeycloakSecurityIT -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `KeycloakSecurityIT` passes 3 tests (Keycloak takes 20–40 s to start the first time), `BUILD SUCCESS`.

If the first test fails with 401 and the log shows an issuer mismatch, print the token's `iss` claim
(decode the middle segment of the JWT with `base64 -d`) and compare it with `realmUrl()`; they must be
identical strings.

- [ ] **Step 5: Commit**

```bash
git add keycloak docker-compose.yml order-service/src/test/java/dev/ledgerline/order/KeycloakSecurityIT.java
git commit -m "Import the ledgerline Keycloak realm and prove real tokens end to end"
```

---

### Task 6: Demos, docs and acceptance run

**Files:**
- Create: `scripts/token.sh`
- Create: `scripts/demo-security.sh`
- Modify: `scripts/demo-kafka-outage.sh`, `scripts/demo-poison-message.sh`, `scripts/demo-risk.sh`
- Modify: `README.md`

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces: runnable demos and documentation. No code interfaces.

- [ ] **Step 1: Token helper**

Create `scripts/token.sh` (sourced by the demos, not executed):

```bash
#!/usr/bin/env bash
# Dev-only: password-grant tokens from the ledgerline-cli client. Usage: source scripts/token.sh; token alice
KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8180}"

token() { # username (password = username)
  curl -s -X POST "$KEYCLOAK_URL/realms/ledgerline/protocol/openid-connect/token" \
    -d grant_type=password -d client_id=ledgerline-cli -d "username=$1" -d "password=$1" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
}

auth() { # username -> header value
  echo "Authorization: Bearer $(token "$1")"
}
```

- [ ] **Step 2: Security demo**

Create `scripts/demo-security.sh`:

```bash
#!/usr/bin/env bash
# Who can do what: traders act on their own account, risk reads everything, ops runs the dead-letter tools.
# Needs: docker compose stack (incl. keycloak), order-service 8081, settlement-service 8082, risk-service 8083.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/token.sh

ORDER_URL="${ORDER_URL:-http://localhost:8081}"
SETTLEMENT_URL="${SETTLEMENT_URL:-http://localhost:8082}"
RISK_URL="${RISK_URL:-http://localhost:8083}"
SYMBOL="$(python3 -c 'import random, string; print("".join(random.choices(string.ascii_uppercase, k=5)))')"
JSON='Content-Type: application/json'

show() { # label, then curl args
  local label=$1; shift
  printf '%-58s HTTP %s\n' "$label" "$(curl -s -o /dev/null -w '%{http_code}' "$@")"
}

echo "== Tokens come from Keycloak (password grant, dev only)"
ALICE=$(auth alice); BOB=$(auth bob); RITA=$(auth rita); OSCAR=$(auth oscar)

echo "== Trading on $SYMBOL: the account is the token's username, not a request field"
show "no token: place order"          -X POST "$ORDER_URL/api/v1/orders" -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":10,\"quantity\":5}"
show "oscar (OPS): place order"        -X POST "$ORDER_URL/api/v1/orders" -H "$OSCAR" -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":10,\"quantity\":5}"
show "alice (TRADER): sell 5 @ 10"     -X POST "$ORDER_URL/api/v1/orders" -H "$ALICE" -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":10,\"quantity\":5}"
show "alice: rest a second sell @ 11"  -X POST "$ORDER_URL/api/v1/orders" -H "$ALICE" -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":11,\"quantity\":1}"
show "bob (TRADER): buy 5 @ 10"        -X POST "$ORDER_URL/api/v1/orders" -H "$BOB"   -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"price\":10,\"quantity\":5}"
show "bob: cancel alice's resting order (order 2)" -X DELETE "$ORDER_URL/api/v1/orders/$SYMBOL/2" -H "$BOB"
show "alice: cancel her own resting order (order 2)" -X DELETE "$ORDER_URL/api/v1/orders/$SYMBOL/2" -H "$ALICE"
sleep 3

echo "== Balances"
show "bob reads bob"                   "$SETTLEMENT_URL/api/v1/accounts/bob/balances" -H "$BOB"
show "alice reads bob"                 "$SETTLEMENT_URL/api/v1/accounts/bob/balances" -H "$ALICE"
show "rita (RISK) reads bob"           "$SETTLEMENT_URL/api/v1/accounts/bob/balances" -H "$RITA"
show "no token reads bob"              "$SETTLEMENT_URL/api/v1/accounts/bob/balances"

echo "== Positions"
show "bob reads bob"                   "$RISK_URL/api/v1/risk/accounts/bob/positions" -H "$BOB"
show "alice reads bob"                 "$RISK_URL/api/v1/risk/accounts/bob/positions" -H "$ALICE"
show "rita (RISK) reads bob"           "$RISK_URL/api/v1/risk/accounts/bob/positions" -H "$RITA"

echo "== Dead-letter ops API"
show "alice lists dead letters"        "$SETTLEMENT_URL/api/v1/ops/dead-letters" -H "$ALICE"
show "rita lists dead letters"         "$SETTLEMENT_URL/api/v1/ops/dead-letters" -H "$RITA"
show "oscar (OPS) lists dead letters"  "$SETTLEMENT_URL/api/v1/ops/dead-letters" -H "$OSCAR"

echo "== Health stays public"
show "no token: order-service health"  "$ORDER_URL/actuator/health"
```

Note the cancel lines assume alice's second order got id 2 in a fresh order-service; if the service has
been running, replace `2` by reading `orderId` from the second sell's response. The expected codes are:
401, 403, 201, 201, 201, 404, 204; 200, 403, 200, 401; 200, 403, 200; 403, 403, 200; 200.

- [ ] **Step 3: Update the other demos to send tokens**

In all three of `demo-kafka-outage.sh`, `demo-poison-message.sh` and `demo-risk.sh`, after `set -euo pipefail`
add:

```bash
cd "$(dirname "$0")/.."
source scripts/token.sh
```

`demo-kafka-outage.sh`:
- replace the two order `curl` calls so the seller is alice and the buyer is bob, each with its header and
  without `accountId`:

```bash
ALICE=$(auth alice); BOB=$(auth bob); RITA=$(auth rita)
curl -s -o /dev/null -w "sell: HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$ALICE" -H "$JSON" \
  -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":50,\"quantity\":5}"
curl -s -o /dev/null -w "buy:  HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$BOB" -H "$JSON" \
  -d "{\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"price\":50,\"quantity\":5}"
```

- in `pending()`, add `-H "$OSCAR"` to the metrics curl and set `OSCAR=$(auth oscar)` next to the other
  tokens (metrics are no longer public);
- replace `outage-buyer` with `bob` in both balance reads and add `-H "$RITA"` to them.

`demo-poison-message.sh`:
- set `OSCAR=$(auth oscar); RITA=$(auth rita)` after sourcing;
- add `-H "$OSCAR"` to the two `dead-letters` GETs and the replay POST;
- add `-H "$RITA"` to the `replay-buyer` balances GET.

`demo-risk.sh`:
- replace `TRADER="trader-$RUN"` and `MARKET="market-$RUN"` with `TRADER=alice` and `MARKET=bob`, and set
  `ALICE=$(auth alice); BOB=$(auth bob); RITA=$(auth rita)`;
- change `order()` to pick the header from the account:

```bash
order() { # account, side, price, quantity
  local header; [ "$1" = alice ] && header=$ALICE || header=$BOB
  curl -s -o /dev/null -w "$1 $2 $4 @ $3: HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$header" -H "$JSON" \
    -d "{\"symbol\":\"$SYMBOL\",\"side\":\"$2\",\"type\":\"LIMIT\",\"price\":$3,\"quantity\":$4}"
}
```

- change `positions()` to `curl -s "$RISK_URL/api/v1/risk/accounts/$1/positions" -H "$RITA"; echo`.
- delete the now-unused `RUN="$(date +%s)"` line.

Run `bash -n` on all four scripts and `chmod +x scripts/demo-security.sh`.

- [ ] **Step 4: README**

In `README.md`:

1. In the Modules table, after the `risk-service` row add:

```markdown
| `ledgerline-security` | Shared JWT resource-server auto-configuration: Keycloak role mapping, username principal, method security, CORS | Spring Security 7 |
```

2. In "Design decisions", after the "Known limits of the risk service." bullet, add:

```markdown
- **The account is the token, not the request.** `POST /orders` has no `accountId`; the engine trades under the
  JWT's `preferred_username`. A trader therefore cannot act for anyone else, and cancelling someone else's order
  returns 404, the same as an unknown order, so nothing leaks.
- **Rules next to the endpoint.** Each controller states its rule with `@PreAuthorize`; the shared module only
  decides what a valid token is (issuer, audience, signature, role mapping) and that everything but health needs one.
```

3. Replace the "Running locally" code block with:

```bash
docker compose up -d                                   # Kafka, kafka-ui, PostgreSQL, Keycloak (imports the realm)
./mvnw install                                         # build and test everything
./mvnw -pl order-service spring-boot:run               # http://localhost:8081
./mvnw -pl settlement-service spring-boot:run          # http://localhost:8082
./mvnw -pl risk-service spring-boot:run                # http://localhost:8083
```

and after the "Kafka UI is at …" line add:

````markdown
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
````

4. Replace the "Try it" block's two order commands with token-bearing ones and drop `accountId`:

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

5. In "Failure demos", add a line: `./scripts/demo-security.sh                         # 401/403/404 matrix across roles and accounts`
   (with the same `DOCKER=…` prefix as the others is not needed; it doesn't use Docker), and change the
   outbox-backlog line to `curl localhost:8081/actuator/metrics/ledgerline.outbox.pending -H "$(auth oscar)"`.

6. In "Testing", add after the "Risk" bullet:

```markdown
- **Security:** MockMvc tests with fake JWTs cover 401 without a token, 403 for the wrong role or another
  trader's account, owner-only cancel, and public health; `KeycloakSecurityIT` starts a real Keycloak, gets
  real tokens, and proves issuer, signature, audience and role mapping end to end.
```

7. In "Roadmap", tick `- [x] B. OAuth2 with Keycloak and roles (trader, risk, ops)`.

- [ ] **Step 5: Full build**

Run: `./mvnw -B clean verify`
Expected: `BUILD SUCCESS`, every test in all seven modules passes with none skipped.

- [ ] **Step 6: Manual acceptance run**

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
docker compose up -d                       # now includes keycloak
until curl -sf localhost:8180/realms/ledgerline >/dev/null; do sleep 2; done
java -Xmx384m -jar order-service/target/order-service-0.1.0-SNAPSHOT.jar &
java -Xmx384m -jar settlement-service/target/settlement-service-0.1.0-SNAPSHOT.jar &
java -Xmx384m -jar risk-service/target/risk-service-0.1.0-SNAPSHOT.jar &
```

Once the three `/actuator/health` endpoints return `UP` (still without a token):
1. `./scripts/demo-security.sh`: every line shows the expected code listed in Step 2.
2. `./scripts/demo-risk.sh`: still works end to end with tokens (alice net 150, one alert per side).
3. `DOCKER=… ./scripts/demo-kafka-outage.sh` and `DOCKER=… ./scripts/demo-poison-message.sh`: still pass.
4. Every service log shows no `ERROR` lines caused by security (grep for `AuthenticationException`,
   `JwtValidationException`, `AccessDenied`).

Stop the services and `docker compose stop` afterwards.

- [ ] **Step 7: Commit**

```bash
git add scripts README.md
git commit -m "Add role and ownership demo, token helper, and security docs"
```

Ask the user before pushing.
