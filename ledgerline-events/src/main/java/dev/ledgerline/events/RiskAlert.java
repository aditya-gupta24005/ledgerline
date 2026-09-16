package dev.ledgerline.events;

import java.math.BigDecimal;

/** Published by the risk service the first time a position's notional goes above its limit. */
public record RiskAlert(
        String accountId,
        String symbol,
        long netQuantity,
        BigDecimal notional,
        BigDecimal limit,
        String tradeId) {
}
