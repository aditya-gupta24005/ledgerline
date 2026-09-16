package dev.ledgerline.risk.query;

import dev.ledgerline.risk.domain.Position;
import java.math.BigDecimal;

/** A position marked at {@code lastPrice}, the symbol's most recent traded price. */
public record PositionView(
        String symbol,
        long netQuantity,
        BigDecimal averageCost,
        BigDecimal lastPrice,
        BigDecimal notional,
        BigDecimal realizedPnl,
        BigDecimal unrealizedPnl) {

    static PositionView of(Position position, BigDecimal markPrice) {
        return new PositionView(
                position.symbol(),
                position.netQuantity(),
                position.averageCost(),
                markPrice,
                markPrice.multiply(BigDecimal.valueOf(Math.abs(position.netQuantity()))),
                position.realizedPnl(),
                position.unrealizedPnl(markPrice));
    }
}
