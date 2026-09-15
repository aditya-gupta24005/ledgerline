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
        // Refresh before sending too: a send can block for seconds while the broker is down,
        // and the backlog should be visible during that time, not only after the run.
        refreshPending();
        Integer publishedCount = transactions.execute(status -> publishBatch());
        refreshPending();
        return publishedCount == null ? 0 : publishedCount;
    }

    private void refreshPending() {
        pending.set(jdbc.queryForObject(
                "SELECT count(*) FROM order_service.outbox_events WHERE published_at IS NULL", Long.class));
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
            // clock_timestamp(), not now(): now() is the transaction start, which can be seconds before the
            // broker ack when a send blocks, and would make rows look published before they were.
            jdbc.update("UPDATE order_service.outbox_events SET published_at = clock_timestamp() WHERE id = ANY(?)",
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
