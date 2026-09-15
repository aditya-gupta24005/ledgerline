package dev.ledgerline.settlement.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.events.Topics;
import dev.ledgerline.settlement.KafkaTestConfiguration;
import dev.ledgerline.settlement.PostgresTestConfiguration;
import dev.ledgerline.settlement.TradeJson;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import dev.ledgerline.settlement.persistence.SettlementInstructionRepository;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@AutoConfigureMockMvc
@Import({PostgresTestConfiguration.class, KafkaTestConfiguration.class})
@Testcontainers(disabledWithoutDocker = true)
class DeadLetterOpsIT {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private SettlementInstructionRepository instructions;

    @Autowired
    private JournalEntryRepository journal;

    @BeforeEach
    void waitForListenerAssignment() {
        listeners.getListenerContainers().forEach(container -> ContainerTestUtils.waitForAssignment(container, 3));
    }

    @Test
    void listsDeadLetteredRecordWithDiagnostics() throws Exception {
        String key = "POISON-" + UUID.randomUUID().toString().substring(0, 8);
        String match = "$[?(@.key == '" + key + "')]";

        kafkaTemplate.send(Topics.TRADES_EXECUTED, key, "{\"bad\":").get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> mockMvc.perform(get("/api/v1/ops/dead-letters?limit=500"))
                .andExpect(status().isOk())
                .andExpect(jsonPath(match + ".payload").value(hasItem("{\"bad\":")))
                .andExpect(jsonPath(match + ".exceptionClass").value(hasItem(containsString("InvalidTradeMessageException"))))
                .andExpect(jsonPath(match + ".originalOffset").value(hasItem(notNullValue())))
                .andExpect(jsonPath(match + ".failedAt").value(hasItem(notNullValue()))));
    }

    @Test
    void replayedDeadLetterIsBookedExactlyOnce() throws Exception {
        String tradeId = "T-ops-" + UUID.randomUUID().toString().substring(0, 8);
        SendResult<String, String> sent = kafkaTemplate
                .send(new ProducerRecord<>(Topics.TRADES_EXECUTED_DLT, 0, "ACME", TradeJson.valid(tradeId)))
                .get(10, TimeUnit.SECONDS);
        long offset = sent.getRecordMetadata().offset();

        mockMvc.perform(post("/api/v1/ops/dead-letters/0/{offset}/replay", offset))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.replayedTo").value(Topics.TRADES_EXECUTED))
                .andExpect(jsonPath("$.partition").value(0))
                .andExpect(jsonPath("$.offset").value(Math.toIntExact(offset)));

        await().atMost(Duration.ofSeconds(20)).until(() -> instructions.existsById(tradeId));
        assertThat(journal.findByTradeId(tradeId)).hasSize(4);

        mockMvc.perform(post("/api/v1/ops/dead-letters/0/{offset}/replay", offset)).andExpect(status().isAccepted());

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6))
                .untilAsserted(() -> assertThat(journal.findByTradeId(tradeId)).hasSize(4));
    }

    @Test
    void unknownOffsetIsNotFound() throws Exception {
        mockMvc.perform(post("/api/v1/ops/dead-letters/0/999999999/replay")).andExpect(status().isNotFound());
    }

    @Test
    void unknownPartitionIsBadRequest() throws Exception {
        mockMvc.perform(post("/api/v1/ops/dead-letters/99/0/replay"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Unknown dead-letter partition"));
    }
}
