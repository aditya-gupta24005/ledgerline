package dev.ledgerline.settlement;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.settlement.domain.SettlementCalendar;
import dev.ledgerline.settlement.domain.TradePostings;
import dev.ledgerline.settlement.persistence.JournalEntry;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import dev.ledgerline.settlement.persistence.SettlementInstruction;
import dev.ledgerline.settlement.persistence.SettlementInstructionRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SettlementService {

    /** US equities moved to T+1 settlement in May 2024. */
    static final int SETTLEMENT_CYCLE_BUSINESS_DAYS = 1;

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    private final SettlementInstructionRepository instructions;
    private final JournalEntryRepository journal;
    private final Clock clock;

    SettlementService(SettlementInstructionRepository instructions, JournalEntryRepository journal, Clock clock) {
        this.instructions = instructions;
        this.journal = journal;
        this.clock = clock;
    }

    /**
     * Books a trade exactly once. Kafka delivers at least once, so the trade id is the idempotency
     * key: a redelivery is skipped, and a concurrent duplicate fails on the instruction's primary
     * key and rolls back.
     *
     * @return {@code true} if the trade was booked, {@code false} if it had already been processed
     */
    @Transactional
    public boolean settle(TradeExecuted trade) {
        if (instructions.existsById(trade.tradeId())) {
            log.info("Trade {} already settled, skipping redelivery", trade.tradeId());
            return false;
        }
        Instant now = clock.instant();
        LocalDate tradeDate = LocalDate.ofInstant(trade.executedAt(), ZoneOffset.UTC);
        LocalDate settlementDate = SettlementCalendar.addBusinessDays(tradeDate, SETTLEMENT_CYCLE_BUSINESS_DAYS);

        instructions.saveAndFlush(new SettlementInstruction(trade, tradeDate, settlementDate, now));
        journal.saveAll(TradePostings.of(trade).stream()
                .map(posting -> new JournalEntry(trade.tradeId(), posting, now))
                .toList());
        return true;
    }
}
