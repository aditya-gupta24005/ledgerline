package dev.ledgerline.risk;

import dev.ledgerline.events.RiskAlert;
import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.risk.domain.Position;
import dev.ledgerline.risk.stream.RiskTopology;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import tools.jackson.databind.json.JsonMapper;

/** Runs the real risk topology in-process with {@link TopologyTestDriver}: no broker, no Docker. */
public final class TopologyFixture implements AutoCloseable {

    public static final JsonMapper JSON = JsonMapper.builder().build();

    private final TopologyTestDriver driver;
    private final TestInputTopic<String, String> trades;
    private final TestOutputTopic<String, String> alerts;

    public TopologyFixture(BigDecimal limit, Path stateDir) {
        StreamsBuilder builder = new StreamsBuilder();
        new RiskTopology(JSON, new RiskProperties(limit, Duration.ofDays(7))).addTo(builder);
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "risk-topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(builder.build(), config, Instant.parse("2026-09-16T10:00:00Z"));
        trades = driver.createInputTopic(Topics.TRADES_EXECUTED, new StringSerializer(), new StringSerializer());
        alerts = driver.createOutputTopic(Topics.RISK_ALERTS, new StringDeserializer(), new StringDeserializer());
    }

    public void pipeTrade(String tradeId, String buyer, String seller, String symbol, long quantity, String price) {
        TradeExecuted trade = new TradeExecuted(tradeId, symbol, "USD", new BigDecimal(price), quantity,
                1, buyer, 2, seller, "BUY", Instant.parse("2026-09-16T10:00:00Z"));
        trades.pipeInput(symbol, JSON.writeValueAsString(trade));
    }

    public void pipeRaw(String value) {
        trades.pipeInput("RAW", value);
    }

    public KeyValueStore<String, Position> positions() {
        return driver.getKeyValueStore(RiskTopology.POSITIONS_STORE);
    }

    public KeyValueStore<String, String> lastPrices() {
        return driver.getKeyValueStore(RiskTopology.LAST_PRICES_STORE);
    }

    public List<RiskAlert> readAlerts() {
        return alerts.readValuesToList().stream().map(json -> JSON.readValue(json, RiskAlert.class)).toList();
    }

    public void advanceWallClock(Duration duration) {
        driver.advanceWallClockTime(duration);
    }

    @Override
    public void close() {
        driver.close();
    }
}
