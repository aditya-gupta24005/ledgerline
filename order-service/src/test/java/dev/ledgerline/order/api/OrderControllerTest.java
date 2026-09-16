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
        String body = placeOrder(trader("alice"), """
                {"accountId": "mallory", "symbol": "SPOOF", "side": "SELL", "type": "LIMIT", "price": 9, "quantity": 1}
                """)
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long orderId = JsonPath.<Number>read(body, "$.orderId").longValue();

        mockMvc.perform(delete("/api/v1/orders/SPOOF/{orderId}", orderId).with(trader("mallory")))
                .andExpect(status().isNotFound());
        mockMvc.perform(delete("/api/v1/orders/SPOOF/{orderId}", orderId).with(trader("alice")))
                .andExpect(status().isNoContent());
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
