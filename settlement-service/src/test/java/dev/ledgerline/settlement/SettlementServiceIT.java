package dev.ledgerline.settlement;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.settlement.domain.SettlementStatus;
import dev.ledgerline.settlement.persistence.AssetBalance;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import dev.ledgerline.settlement.persistence.SettlementInstructionRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Runs against a real PostgreSQL (with the Flyway schema) in Docker; skipped when Docker is unavailable. */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@Testcontainers(disabledWithoutDocker = true)
class SettlementServiceIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @Autowired
    private SettlementService settlementService;

    @Autowired
    private JournalEntryRepository journal;

    @Autowired
    private SettlementInstructionRepository instructions;

    @Test
    void booksATradeExactlyOnceWithBalancedEntriesAndT1Settlement() {
        TradeExecuted trade = new TradeExecuted("T-it-1", "ACME", "USD", new BigDecimal("250.0000"), 10,
                1, "alice", 2, "bob", "BUY", Instant.parse("2026-09-18T14:00:00Z")); // a Friday

        assertThat(settlementService.settle(trade)).isTrue();
        assertThat(settlementService.settle(trade)).as("redelivery must be ignored").isFalse();

        assertThat(journal.findByTradeId("T-it-1")).hasSize(4);
        assertThat(instructions.findById("T-it-1")).hasValueSatisfying(instruction -> {
            assertThat(instruction.getSettlementDate()).isEqualTo(LocalDate.of(2026, 9, 21));
            assertThat(instruction.getStatus()).isEqualTo(SettlementStatus.PENDING);
        });

        List<AssetBalance> alice = journal.balancesFor("alice");
        assertThat(alice).extracting(AssetBalance::asset).containsExactly("ACME", "USD");
        assertThat(alice.get(0).balance()).isEqualByComparingTo("10");
        assertThat(alice.get(1).balance()).isEqualByComparingTo("-2500");
    }
}
