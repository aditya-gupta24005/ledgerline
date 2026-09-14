package dev.ledgerline.matching;

/** Mutable engine-internal order. Also a node in its price level's intrusive linked list. */
final class Order {

    final long orderId;
    final String accountId;
    final Side side;
    final long priceTicks;
    final long quantity;

    long remaining;

    PriceLevel level;
    Order prev;
    Order next;

    Order(long orderId, String accountId, Side side, long priceTicks, long quantity) {
        this.orderId = orderId;
        this.accountId = accountId;
        this.side = side;
        this.priceTicks = priceTicks;
        this.quantity = quantity;
        this.remaining = quantity;
    }
}
