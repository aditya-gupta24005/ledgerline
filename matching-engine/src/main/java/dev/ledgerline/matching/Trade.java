package dev.ledgerline.matching;

/** A single fill between an incoming (aggressor) order and a resting order, at the resting price. */
public record Trade(
        long tradeId,
        String symbol,
        long priceTicks,
        long quantity,
        long buyOrderId,
        String buyAccountId,
        long sellOrderId,
        String sellAccountId,
        Side aggressorSide) {
}
