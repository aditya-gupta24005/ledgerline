package dev.ledgerline.order.events;

import dev.ledgerline.events.TradeExecuted;

public interface TradeEventPublisher {

    void publish(TradeExecuted trade);
}
