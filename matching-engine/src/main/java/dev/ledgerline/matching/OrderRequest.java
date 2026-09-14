package dev.ledgerline.matching;

/**
 * An order as submitted to the engine. Prices are integer ticks; converting to and from decimal
 * prices is the caller's job, which keeps {@code BigDecimal} allocation out of the matching loop.
 */
public record OrderRequest(
        String accountId,
        String symbol,
        Side side,
        OrderType type,
        long priceTicks,
        long quantity) {

    public static OrderRequest limit(String accountId, String symbol, Side side, long priceTicks, long quantity) {
        return new OrderRequest(accountId, symbol, side, OrderType.LIMIT, priceTicks, quantity);
    }

    public static OrderRequest market(String accountId, String symbol, Side side, long quantity) {
        return new OrderRequest(accountId, symbol, side, OrderType.MARKET, 0, quantity);
    }
}
