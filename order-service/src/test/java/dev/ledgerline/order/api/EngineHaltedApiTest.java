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
