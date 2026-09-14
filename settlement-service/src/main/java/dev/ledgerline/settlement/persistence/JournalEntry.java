package dev.ledgerline.settlement.persistence;

import dev.ledgerline.settlement.domain.Posting;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;

/** Append-only ledger line. Balances are always derived by summing entries, never stored. */
@Entity
@Table(name = "journal_entries")
public class JournalEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String tradeId;
    private String accountId;
    private String asset;
    private BigDecimal amount;
    private Instant postedAt;

    protected JournalEntry() {
    }

    public JournalEntry(String tradeId, Posting posting, Instant postedAt) {
        this.tradeId = tradeId;
        this.accountId = posting.accountId();
        this.asset = posting.asset();
        this.amount = posting.amount();
        this.postedAt = postedAt;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getAsset() {
        return asset;
    }

    public BigDecimal getAmount() {
        return amount;
    }
}
