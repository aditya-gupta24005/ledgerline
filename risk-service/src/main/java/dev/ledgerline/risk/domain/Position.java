package dev.ledgerline.risk.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Net position of one account in one symbol, with average-cost P&L.
 *
 * <p>Average cost changes only when a position is opened or increased. Reducing a position realises P&L
 * against the average cost. Going through zero closes the old position, and the remainder opens at the
 * fill price.
 */
public record Position(
        String accountId,
        String symbol,
        long netQuantity,
        BigDecimal averageCost,
        BigDecimal realizedPnl,
        BigDecimal lastPrice,
        BigDecimal notional,
        String lastTradeId,
        boolean limitBreachedByLastFill) {

    public static final int SCALE = 4;

    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE);

    public static Position empty() {
        return new Position(null, null, 0, ZERO, ZERO, ZERO, ZERO, null, false);
    }

    public Position apply(Fill fill, BigDecimal notionalLimit) {
        BigDecimal price = fill.price().setScale(SCALE, RoundingMode.HALF_EVEN);
        long delta = fill.signedQuantity();
        long newQuantity = Math.addExact(netQuantity, delta);

        BigDecimal newAverageCost = averageCost;
        BigDecimal newRealizedPnl = realizedPnl;
        if (netQuantity == 0 || Long.signum(netQuantity) == Long.signum(delta)) {
            BigDecimal totalCost = averageCost.multiply(BigDecimal.valueOf(Math.abs(netQuantity)))
                    .add(price.multiply(BigDecimal.valueOf(Math.abs(delta))));
            newAverageCost = totalCost.divide(BigDecimal.valueOf(Math.abs(newQuantity)), SCALE, RoundingMode.HALF_EVEN);
        } else {
            long closedQuantity = Math.min(Math.abs(netQuantity), Math.abs(delta));
            newRealizedPnl = realizedPnl.add(price.subtract(averageCost)
                    .multiply(BigDecimal.valueOf(closedQuantity * Long.signum(netQuantity))));
            if (newQuantity == 0) {
                newAverageCost = ZERO;
            } else if (Long.signum(newQuantity) != Long.signum(netQuantity)) {
                newAverageCost = price;
            }
        }

        BigDecimal newNotional = price.multiply(BigDecimal.valueOf(Math.abs(newQuantity)));
        boolean crossedLimit = notional.compareTo(notionalLimit) <= 0 && newNotional.compareTo(notionalLimit) > 0;
        return new Position(fill.accountId(), fill.symbol(), newQuantity, newAverageCost, newRealizedPnl, price,
                newNotional, fill.tradeId(), crossedLimit);
    }

    /** P&L if the whole position were closed at {@code markPrice}. */
    public BigDecimal unrealizedPnl(BigDecimal markPrice) {
        return markPrice.subtract(averageCost).multiply(BigDecimal.valueOf(netQuantity));
    }
}
