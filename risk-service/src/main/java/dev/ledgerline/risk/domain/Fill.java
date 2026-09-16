package dev.ledgerline.risk.domain;

import dev.ledgerline.events.TradeExecuted;
import java.math.BigDecimal;
import java.time.Instant;

/** One account's side of a trade: positive quantity for the buyer, negative for the seller. */
public record Fill(String tradeId, String accountId, String symbol, long signedQuantity, BigDecimal price, Instant executedAt) {

    public static Fill buyerSide(TradeExecuted trade) {
        return new Fill(trade.tradeId(), trade.buyAccountId(), trade.symbol(), trade.quantity(), trade.price(),
                trade.executedAt());
    }

    public static Fill sellerSide(TradeExecuted trade) {
        return new Fill(trade.tradeId(), trade.sellAccountId(), trade.symbol(), -trade.quantity(), trade.price(),
                trade.executedAt());
    }
}
