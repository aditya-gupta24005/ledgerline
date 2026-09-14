package dev.ledgerline.matching;

import java.util.List;

/**
 * Outcome of submitting one order.
 *
 * @param restingQuantity quantity left on the book after matching (always 0 for market orders)
 * @param rejectReason    set only when {@code status} is {@link OrderStatus#REJECTED}
 */
public record MatchResult(
        long orderId,
        OrderStatus status,
        long filledQuantity,
        long restingQuantity,
        List<Trade> trades,
        String rejectReason) {

    public MatchResult {
        trades = List.copyOf(trades);
    }

    static MatchResult rejected(long orderId, String reason) {
        return new MatchResult(orderId, OrderStatus.REJECTED, 0, 0, List.of(), reason);
    }
}
