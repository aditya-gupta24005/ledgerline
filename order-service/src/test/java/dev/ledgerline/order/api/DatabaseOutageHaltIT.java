package dev.ledgerline.order.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.order.PostgresTestConfiguration;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * With the real outbox writer, a database outage must halt order entry before the caller's engine timeout
 * (5 s) expires. Otherwise the engine thread stays blocked waiting for a connection, the caller gets a generic
 * 500, and every later command, including book reads, queues behind it.
 */
@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@AutoConfigureMockMvc
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext
class DatabaseOutageHaltIT {

    private static final Duration ENGINE_TIMEOUT = Duration.ofSeconds(5);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private PostgreSQLContainer postgres;

    private ResultActions placeOrder(String account, String side) throws Exception {
        return mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
                {"accountId": "%s", "symbol": "DBOUT", "side": "%s", "type": "LIMIT", "price": 20, "quantity": 1}
                """.formatted(account, side)));
    }

    @Test
    void databaseOutageHaltsOrderEntryWithin503BeforeTheEngineTimeoutAndKeepsTheBookReadable() throws Exception {
        // Warm the connection pool with a real trade first, as in a service that has been running. A pool that has
        // never started fails fast on its first connection attempt, which hides the bug.
        placeOrder("warmup-seller", "SELL").andExpect(status().isCreated());
        placeOrder("warmup-buyer", "BUY").andExpect(status().isCreated());
        placeOrder("outage-seller", "SELL").andExpect(status().isCreated());

        // Stop the way `docker compose stop` does (graceful shutdown), not Testcontainers' kill: Postgres then
        // terminates client connections, and new connections must be opened during the outage.
        postgres.getDockerClient().stopContainerCmd(postgres.getContainerId()).exec();
        // Let the pooled connection sit idle past Hikari's 500 ms alive-bypass window. Borrowing it then triggers
        // validation, which fails, evicts it, and makes the pool wait for a brand-new connection, as in the real
        // outage. A connection used within the window is handed out unchecked and fails immediately instead.
        Thread.sleep(Duration.ofSeconds(1));

        long startedAt = System.nanoTime();
        placeOrder("outage-buyer", "BUY")
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("Engine halted"));
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).isLessThan(ENGINE_TIMEOUT.minusSeconds(1));

        placeOrder("outage-other", "SELL").andExpect(status().isServiceUnavailable());
        mockMvc.perform(get("/api/v1/books/DBOUT")).andExpect(status().isOk());
    }
}
