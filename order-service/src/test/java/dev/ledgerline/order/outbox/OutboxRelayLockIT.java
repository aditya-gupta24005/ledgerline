package dev.ledgerline.order.outbox;

import static dev.ledgerline.order.outbox.OutboxWriterIT.trade;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.ledgerline.order.PostgresTestConfiguration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
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

@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class OutboxRelayLockIT {

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private OutboxRelay relay;

    @Autowired
    private JdbcTemplate jdbc;

    @MockitoBean
    private KafkaTemplate<String, String> kafka;

    @BeforeEach
    void clearOutbox() {
        jdbc.update("DELETE FROM order_service.outbox_events");
    }

    @Test
    void secondRelayRunSkipsWhileAnotherHoldsTheAdvisoryLock() throws Exception {
        outboxWriter.append(List.of(trade("T-l-1", "ACME")));
        CountDownLatch sendStarted = new CountDownLatch(1);
        CountDownLatch releaseSend = new CountDownLatch(1);
        when(kafka.send(ArgumentMatchers.<ProducerRecord<String, String>>any())).thenAnswer(invocation -> {
            sendStarted.countDown();
            releaseSend.await(10, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(null);
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> firstRun = executor.submit(relay::runOnce);
            assertThat(sendStarted.await(10, TimeUnit.SECONDS)).isTrue();

            int secondRun = relay.runOnce();
            releaseSend.countDown();

            assertThat(secondRun).isZero();
            assertThat(firstRun.get(10, TimeUnit.SECONDS)).isEqualTo(1);
            verify(kafka, times(1)).send(ArgumentMatchers.<ProducerRecord<String, String>>any());
        } finally {
            executor.shutdownNow();
        }
    }
}
