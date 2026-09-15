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
