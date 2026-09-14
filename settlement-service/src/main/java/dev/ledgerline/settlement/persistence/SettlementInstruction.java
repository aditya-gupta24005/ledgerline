package dev.ledgerline.settlement.persistence;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.settlement.domain.SettlementStatus;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import org.springframework.data.domain.Persistable;

/**
 * One row per trade, keyed by trade id. Implements {@link Persistable} so saving a new instruction
 * issues a plain INSERT (and a duplicate trips the primary key) instead of Spring Data's default
 * select-then-merge for entities with assigned ids.
 */
@Entity
@Table(name = "settlement_instructions")
public class SettlementInstruction implements Persistable<String> {

    @Id
    private String tradeId;

    private String symbol;
    private String buyAccountId;
    private String sellAccountId;
    private BigDecimal price;
    private long quantity;
    private String currency;
    private LocalDate tradeDate;
    private LocalDate settlementDate;

    @Enumerated(EnumType.STRING)
    private SettlementStatus status;

    private Instant createdAt;

    @Transient
    private boolean newEntity = true;

    protected SettlementInstruction() {
    }

    public SettlementInstruction(TradeExecuted trade, LocalDate tradeDate, LocalDate settlementDate, Instant createdAt) {
        this.tradeId = trade.tradeId();
        this.symbol = trade.symbol();
        this.buyAccountId = trade.buyAccountId();
        this.sellAccountId = trade.sellAccountId();
        this.price = trade.price();
        this.quantity = trade.quantity();
        this.currency = trade.currency();
        this.tradeDate = tradeDate;
        this.settlementDate = settlementDate;
        this.status = SettlementStatus.PENDING;
        this.createdAt = createdAt;
    }

    @Override
    public String getId() {
        return tradeId;
    }

    @Override
    public boolean isNew() {
        return newEntity;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        newEntity = false;
    }

    public LocalDate getTradeDate() {
        return tradeDate;
    }

    public LocalDate getSettlementDate() {
        return settlementDate;
    }

    public SettlementStatus getStatus() {
        return status;
    }
}
