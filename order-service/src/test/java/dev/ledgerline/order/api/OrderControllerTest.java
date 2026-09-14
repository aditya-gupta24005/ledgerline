package dev.ledgerline.order.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.order.events.TradeEventPublisher;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * Exercises the full stack from HTTP through the sequencer and the real matching engine, with only
 * Kafka publishing mocked. The engine is shared across tests, so each test uses its own symbol.
 */
@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
@AutoConfigureMockMvc
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TradeEventPublisher publisher;

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

        verifyNoInteractions(publisher);
    }

    @Test
    void crossingOrdersTradeAndPublishTheTrade() throws Exception {
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

        ArgumentCaptor<TradeExecuted> published = ArgumentCaptor.forClass(TradeExecuted.class);
        verify(publisher).publish(published.capture());
        TradeExecuted trade = published.getValue();
        assertThat(trade.symbol()).isEqualTo("CROSS");
        assertThat(trade.price()).isEqualByComparingTo("101.50");
        assertThat(trade.quantity()).isEqualTo(5);
        assertThat(trade.buyAccountId()).isEqualTo("bob");
        assertThat(trade.sellAccountId()).isEqualTo("alice");
        assertThat(trade.currency()).isEqualTo("USD");
        assertThat(trade.aggressorSide()).isEqualTo("BUY");
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
