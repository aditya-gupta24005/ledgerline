package dev.ledgerline.matching;

public enum OrderType {
    /** Trades at the limit price or better; any remainder rests on the book. */
    LIMIT,
    /** Trades immediately at the best available prices; any remainder is cancelled. */
    MARKET
}
