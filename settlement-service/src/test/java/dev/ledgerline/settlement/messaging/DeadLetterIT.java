package dev.ledgerline.settlement.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.ledgerline.events.Topics;
import dev.ledgerline.settlement.KafkaProbe;
import dev.ledgerline.settlement.KafkaTestConfiguration;
import dev.ledgerline.settlement.PostgresTestConfiguration;
import dev.ledgerline.settlement.TradeJson;
import dev.ledgerline.settlement.persistence.SettlementInstructionRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest
@Import({PostgresTestConfiguration.class, KafkaTestConfiguration.class})
@Testcontainers(disabledWithoutDocker = true)
class DeadLetterIT {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private KafkaContainer kafka;

    @Autowired
    private SettlementInstructionRepository instructions;

    @BeforeEach
    void waitForListenerAssignment() {
        listeners.getListenerContainers().forEach(container -> ContainerTestUtils.waitForAssignment(container, 3));
    }

    @Test
    void malformedMessageGoesToDeadLetterTopicWithoutRetries() throws Exception {
        String key = "POISON-" + UUID.randomUUID().toString().substring(0, 8);
        long sentAt = System.nanoTime();

        SendResult<String, String> sent = kafkaTemplate.send(Topics.TRADES_EXECUTED, key, "{\"bad\":").get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> dead = KafkaProbe.poll(kafka.getBootstrapServers(), Topics.TRADES_EXECUTED_DLT,
                record -> key.equals(record.key()), Duration.ofSeconds(20)).orElseThrow();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - sentAt);

        assertThat(dead.value()).isEqualTo("{\"bad\":");
        assertThat(dead.partition()).isEqualTo(sent.getRecordMetadata().partition());
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_FQCN) + header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .contains("InvalidTradeMessageException");
        // With retries this would take at least 7 s (1 s + 2 s + 4 s).
        assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void validTradeBehindAPoisonMessageIsStillSettled() throws Exception {
        String tradeId = "T-dlt-" + UUID.randomUUID().toString().substring(0, 8);

        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", "{\"bad\":").get(10, TimeUnit.SECONDS);
        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", TradeJson.valid(tradeId)).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).until(() -> instructions.existsById(tradeId));
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
