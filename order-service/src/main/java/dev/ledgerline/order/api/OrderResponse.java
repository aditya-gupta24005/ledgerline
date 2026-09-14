package dev.ledgerline.order.api;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.matching.MatchResult;
import dev.ledgerline.matching.OrderStatus;
import dev.ledgerline.order.engine.OrderOutcome;
import java.math.BigDecimal;
import java.util.List;

public record OrderResponse(
        long orderId,
        OrderStatus status,
        long filledQuantity,
        long restingQuantity,
        List<Fill> fills) {

    public record Fill(String tradeId, BigDecimal price, long quantity) {

        static Fill from(TradeExecuted trade) {
            return new Fill(trade.tradeId(), trade.price(), trade.quantity());
        }
    }

    static OrderResponse from(OrderOutcome outcome) {
        MatchResult result = outcome.result();
        return new OrderResponse(
                result.orderId(),
                result.status(),
                result.filledQuantity(),
                result.restingQuantity(),
                outcome.trades().stream().map(Fill::from).toList());
    }
}
