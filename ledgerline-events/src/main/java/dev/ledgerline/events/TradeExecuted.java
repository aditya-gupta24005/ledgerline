package dev.ledgerline.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Published by the order service for every fill produced by the matching engine.
 *
 * <p>Records are keyed by symbol, so all trades for one instrument land on the same partition and
 * are consumed in the order they were matched.
 */
public record TradeExecuted(
        String tradeId,
        String symbol,
        String currency,
        BigDecimal price,
        long quantity,
        long buyOrderId,
        String buyAccountId,
        long sellOrderId,
        String sellAccountId,
        String aggressorSide,
        Instant executedAt) {

    public TradeExecuted {
        Objects.requireNonNull(tradeId, "tradeId");
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(currency, "currency");
        Objects.requireNonNull(price, "price");
        Objects.requireNonNull(buyAccountId, "buyAccountId");
        Objects.requireNonNull(sellAccountId, "sellAccountId");
        Objects.requireNonNull(executedAt, "executedAt");
        if (price.signum() <= 0) {
            throw new IllegalArgumentException("price must be positive");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
    }
}
