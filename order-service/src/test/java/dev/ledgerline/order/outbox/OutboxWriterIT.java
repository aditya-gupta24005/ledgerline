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
