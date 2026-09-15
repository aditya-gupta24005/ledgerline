package dev.ledgerline.order.outbox;

import static dev.ledgerline.order.outbox.OutboxWriterIT.trade;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.order.PostgresTestConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class OutboxRelayFailureIT {

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

    @MockitoBean
    private KafkaTemplate<String, String> kafka;

    private final List<String> sentTradeIds = new CopyOnWriteArrayList<>();

    @BeforeEach
    void clearOutbox() {
        jdbc.update("DELETE FROM order_service.outbox_events");
    }

    @Test
    void stopsAtFirstFailedSendAndResumesFromThatRowInOrder() {
        for (int i = 1; i <= 5; i++) {
            outboxWriter.append(List.of(trade("T-f-" + i, "ACME")));
        }
        AtomicInteger calls = new AtomicInteger();
        when(kafka.send(ArgumentMatchers.<ProducerRecord<String, String>>any())).thenAnswer(invocation -> {
            recordSent(invocation.getArgument(0));
            return calls.incrementAndGet() == 3
                    ? CompletableFuture.failedFuture(new KafkaException("broker down"))
                    : CompletableFuture.completedFuture(null);
        });

        assertThat(relay.runOnce()).isEqualTo(2);

        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT published_at, attempts, last_error FROM order_service.outbox_events ORDER BY id");
        assertThat(rows).extracting(row -> row.get("published_at") != null)
                .containsExactly(true, true, false, false, false);
        assertThat(rows.get(2).get("attempts")).isEqualTo(1);
        assertThat((String) rows.get(2).get("last_error")).contains("broker down");
        assertThat(rows.get(3).get("attempts")).isEqualTo(0);
        assertThat(meterRegistry.get("ledgerline.outbox.pending").gauge().value()).isEqualTo(3.0);
        assertThat(meterRegistry.get("ledgerline.outbox.publish.failures").counter().count()).isEqualTo(1.0);

        reset(kafka);
        when(kafka.send(ArgumentMatchers.<ProducerRecord<String, String>>any())).thenAnswer(invocation -> {
            recordSent(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });

        assertThat(relay.runOnce()).isEqualTo(3);
        assertThat(sentTradeIds).containsExactly("T-f-1", "T-f-2", "T-f-3", "T-f-3", "T-f-4", "T-f-5");
        assertThat(meterRegistry.get("ledgerline.outbox.pending").gauge().value()).isZero();
    }

    private void recordSent(ProducerRecord<String, String> record) {
        sentTradeIds.add(jsonMapper.readValue(record.value(), TradeExecuted.class).tradeId());
    }
}
