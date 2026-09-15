package dev.ledgerline.settlement.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.ledgerline.events.Topics;
import dev.ledgerline.settlement.KafkaProbe;
import dev.ledgerline.settlement.KafkaTestConfiguration;
import dev.ledgerline.settlement.PostgresTestConfiguration;
import dev.ledgerline.settlement.SettlementService;
import dev.ledgerline.settlement.TradeJson;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = "ledgerline.settlement.retry.initial-interval=100ms")
@Import({PostgresTestConfiguration.class, KafkaTestConfiguration.class})
@Testcontainers(disabledWithoutDocker = true)
class TransientRetryIT {

    @MockitoBean
    private SettlementService settlementService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private KafkaContainer kafka;

    @Test
    void transientFailureIsRetriedAndNotDeadLettered() throws Exception {
        when(settlementService.settle(any()))
                .thenThrow(new TransientDataAccessResourceException("db blip"))
                .thenThrow(new TransientDataAccessResourceException("db blip"))
                .thenReturn(true);
        listeners.getListenerContainers().forEach(container -> ContainerTestUtils.waitForAssignment(container, 3));
        String tradeId = "T-retry-" + UUID.randomUUID().toString().substring(0, 8);

        kafkaTemplate.send(Topics.TRADES_EXECUTED, "ACME", TradeJson.valid(tradeId)).get(10, TimeUnit.SECONDS);

        verify(settlementService, timeout(10_000).times(3)).settle(argThat(trade -> tradeId.equals(trade.tradeId())));
        assertThat(KafkaProbe.poll(kafka.getBootstrapServers(), Topics.TRADES_EXECUTED_DLT,
                record -> record.value() != null && record.value().contains(tradeId), Duration.ofSeconds(3)))
                .isEmpty();
    }
}
