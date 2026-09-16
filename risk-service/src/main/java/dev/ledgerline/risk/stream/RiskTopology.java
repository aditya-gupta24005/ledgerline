package dev.ledgerline.risk.stream;

import dev.ledgerline.events.RiskAlert;
import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.risk.RiskProperties;
import dev.ledgerline.risk.domain.Fill;
import dev.ledgerline.risk.domain.Position;
import java.math.BigDecimal;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import tools.jackson.databind.json.JsonMapper;

/**
 * trades → de-duplicate → two fills per trade → positions per account and symbol → limit alerts, plus the
 * last traded price per symbol.
 */
public final class RiskTopology {

    public static final String SEEN_TRADES_STORE = "seen-trades";
    public static final String POSITIONS_STORE = "positions";
    public static final String LAST_PRICES_STORE = "last-prices";

    private final JsonMapper jsonMapper;
    private final RiskProperties properties;

    public RiskTopology(JsonMapper jsonMapper, RiskProperties properties) {
        this.jsonMapper = jsonMapper;
        this.properties = properties;
    }

    public static String positionKey(String accountId, String symbol) {
        return accountId + "|" + symbol;
    }

    public void addTo(StreamsBuilder builder) {
        Serde<Fill> fillSerde = JsonSerdes.of(Fill.class, jsonMapper);
        Serde<Position> positionSerde = JsonSerdes.of(Position.class, jsonMapper);
        Serde<RiskAlert> alertSerde = JsonSerdes.of(RiskAlert.class, jsonMapper);
        BigDecimal limit = properties.positionNotionalLimit();

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(SEEN_TRADES_STORE), Serdes.String(), Serdes.Long()));

        KStream<String, TradeExecuted> trades = builder
                .stream(Topics.TRADES_EXECUTED, Consumed.with(Serdes.String(), Serdes.String()))
                .processValues(() -> new DeduplicatingTradeParser(jsonMapper, properties.dedupeRetention()),
                        SEEN_TRADES_STORE);

        KTable<String, Position> positions = trades
                .flatMap((symbol, trade) -> List.of(
                        KeyValue.pair(positionKey(trade.buyAccountId(), trade.symbol()), Fill.buyerSide(trade)),
                        KeyValue.pair(positionKey(trade.sellAccountId(), trade.symbol()), Fill.sellerSide(trade))))
                .groupByKey(Grouped.with("fills-by-position", Serdes.String(), fillSerde))
                .aggregate(Position::empty, (key, fill, position) -> position.apply(fill, limit),
                        Materialized.<String, Position, KeyValueStore<Bytes, byte[]>>as(POSITIONS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(positionSerde)
                                // Every update must reach the alert filter. With caching, two fills inside one
                                // commit interval would collapse into one update and a limit crossing could be lost.
                                .withCachingDisabled());

        positions.toStream()
                .filter((key, position) -> position != null && position.limitBreachedByLastFill())
                .mapValues(position -> new RiskAlert(position.accountId(), position.symbol(), position.netQuantity(),
                        position.notional(), limit, position.lastTradeId()))
                .to(Topics.RISK_ALERTS, Produced.with(Serdes.String(), alertSerde));

        trades
                .selectKey((key, trade) -> trade.symbol())
                .mapValues(trade -> trade.price().toPlainString())
                .groupByKey(Grouped.with("prices-by-symbol", Serdes.String(), Serdes.String()))
                .reduce((previous, latest) -> latest,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(LAST_PRICES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()));
    }
}
