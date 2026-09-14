package dev.ledgerline.settlement.domain;

import java.math.BigDecimal;

/** A signed movement of one asset (a currency or a security) for one account. */
public record Posting(String accountId, String asset, BigDecimal amount) {
}
