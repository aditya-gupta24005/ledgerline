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
