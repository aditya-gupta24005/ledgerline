package dev.ledgerline.risk;

import static dev.ledgerline.risk.TestUsers.trader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.events.RiskAlert;
import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Real broker, real Kafka Streams with exactly_once_v2, real REST endpoint. */
@SpringBootTest(properties = {
        "ledgerline.risk.position-notional-limit=1000",
        "spring.kafka.streams.state-dir=${java.io.tmpdir}/risk-service-it-${random.uuid}"
})
@AutoConfigureMockMvc
@Import(KafkaTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class RiskServiceIT {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaContainer kafka;

    @Test
    void tradesBecomePositionsDuplicatesAreIgnoredAndCrossingTheLimitAlertsOnce() throws Exception {
        String run = UUID.randomUUID().toString().substring(0, 8);
        String buyer = "buyer-" + run;
        String seller = "seller-" + run;
        String tradeJson = TopologyFixture.JSON.writeValueAsString(new TradeExecuted("T-" + run, "ACME", "USD",
                new BigDecimal("100.0000"), 11, 1, buyer, 2, seller, "BUY", Instant.parse("2026-09-16T10:00:00Z")));

        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", tradeJson).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", tradeJson).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> mockMvc
                .perform(get("/api/v1/risk/accounts/{account}/positions", buyer).with(trader(buyer)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].symbol").value("ACME"))
                .andExpect(jsonPath("$[0].netQuantity").value(11)));
        mockMvc.perform(get("/api/v1/risk/accounts/{account}/positions", seller).with(trader(seller)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].netQuantity").value(-11));

        List<RiskAlert> alerts = alertsForRun(run, Duration.ofSeconds(15));
        assertThat(alerts).as("one alert per side, the duplicate adds none")
                .extracting(RiskAlert::accountId)
                .containsExactlyInAnyOrder(buyer, seller);
        assertThat(alerts).allSatisfy(alert -> assertThat(alert.notional()).isEqualByComparingTo("1100"));
    }

    private List<RiskAlert> alertsForRun(String run, Duration window) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "risk-it-" + run,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        List<RiskAlert> alerts = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(Topics.RISK_ALERTS));
            long deadline = System.nanoTime() + window.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    RiskAlert alert = TopologyFixture.JSON.readValue(record.value(), RiskAlert.class);
                    if (alert.tradeId().equals("T-" + run)) {
                        alerts.add(alert);
                    }
                }
            }
        }
        return alerts;
    }
}
