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
