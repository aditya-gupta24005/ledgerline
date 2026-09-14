package dev.ledgerline.matching;

public enum OrderStatus {
    /** Rested on the book without trading. */
    NEW,
    /** Traded part of its quantity; the remainder is resting on the book. */
    PARTIALLY_FILLED,
    FILLED,
    /** Market order whose unfilled remainder was cancelled because liquidity ran out. */
    CANCELLED,
    REJECTED
}
