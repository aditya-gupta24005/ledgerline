package dev.ledgerline.order.outbox;

import dev.ledgerline.events.TradeExecuted;
import java.util.List;

/**
 * Durably records trades for later publication. Implementations must write all trades of one call
 * atomically, or throw and write none.
 */
public interface OutboxWriter {

    void append(List<TradeExecuted> trades);
}
