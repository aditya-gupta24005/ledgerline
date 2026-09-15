package dev.ledgerline.order.outbox;

import static dev.ledgerline.order.outbox.OutboxWriterIT.trade;
import static org.assertj.core.api.Assertions.assertThat;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.order.KafkaTestConfiguration;
import dev.ledgerline.order.PostgresTestConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = "ledgerline.scheduling.enabled=false")
@Import({PostgresTestConfiguration.class, KafkaTestConfiguration.class})
@Testcontainers(disabledWithoutDocker = true)
class OutboxRelayIT {

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private OutboxRelay relay;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private KafkaContainer kafka;

    @BeforeEach
    void clearOutbox() {
        jdbc.update("DELETE FROM order_service.outbox_events");
    }

    @Test
    void publishesPendingRowsInMatchOrderPerSymbolAndMarksThemPublished() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        List<TradeExecuted> matched = List.of(
                trade("T-" + run + "-1", "ACME"),
                trade("T-" + run + "-2", "GLOBX"),
                trade("T-" + run + "-3", "ACME"),
                trade("T-" + run + "-4", "GLOBX"),
                trade("T-" + run + "-5", "ACME"));
        matched.forEach(trade -> outboxWriter.append(List.of(trade)));

        assertThat(relay.runOnce()).isEqualTo(5);

        List<ConsumerRecord<String, String>> received = consumeTradesWithPrefix("T-" + run + "-", 5);
        Map<String, List<String>> tradeIdsBySymbol = received.stream().collect(Collectors.groupingBy(
                ConsumerRecord::key,
                Collectors.mapping(record -> tradeId(record.value()), Collectors.toList())));
        assertThat(tradeIdsBySymbol.get("ACME")).containsExactly("T-" + run + "-1", "T-" + run + "-3", "T-" + run + "-5");
        assertThat(tradeIdsBySymbol.get("GLOBX")).containsExactly("T-" + run + "-2", "T-" + run + "-4");
        assertThat(received).allSatisfy(record -> assertThat(
                new String(record.headers().lastHeader(OutboxRelay.EVENT_TYPE_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("TradeExecuted"));

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM order_service.outbox_events WHERE published_at IS NULL", Long.class)).isZero();
        assertThat(meterRegistry.get("ledgerline.outbox.pending").gauge().value()).isZero();
        assertThat(meterRegistry.get("ledgerline.outbox.published").counter().count()).isGreaterThanOrEqualTo(5);
    }

    @Test
    void runWithNothingPendingPublishesNothing() {
        assertThat(relay.runOnce()).isZero();
    }

    private String tradeId(String json) {
        return jsonMapper.readValue(json, TradeExecuted.class).tradeId();
    }

    private List<ConsumerRecord<String, String>> consumeTradesWithPrefix(String tradeIdPrefix, int expected) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-relay-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(Topics.TRADES_EXECUTED));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (received.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (tradeId(record.value()).startsWith(tradeIdPrefix)) {
                        received.add(record);
                    }
                }
            }
        }
        assertThat(received).hasSize(expected);
        return received;
    }
}
