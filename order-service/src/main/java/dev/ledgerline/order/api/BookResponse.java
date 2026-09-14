package dev.ledgerline.order.api;

import dev.ledgerline.matching.BookSnapshot;
import dev.ledgerline.order.engine.Prices;
import java.math.BigDecimal;
import java.util.List;

public record BookResponse(String symbol, List<Level> bids, List<Level> asks) {

    public record Level(BigDecimal price, long quantity, int orders) {

        static Level from(BookSnapshot.LevelView level) {
            return new Level(Prices.fromTicks(level.priceTicks()), level.quantity(), level.orderCount());
        }
    }

    static BookResponse from(BookSnapshot snapshot) {
        return new BookResponse(
                snapshot.symbol(),
                snapshot.bids().stream().map(Level::from).toList(),
                snapshot.asks().stream().map(Level::from).toList());
    }
}
