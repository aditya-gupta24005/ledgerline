package dev.ledgerline.matching;

import java.util.List;

/** Aggregated depth view: bids best (highest) first, asks best (lowest) first. */
public record BookSnapshot(String symbol, List<LevelView> bids, List<LevelView> asks) {

    public record LevelView(long priceTicks, long quantity, int orderCount) {
    }
}
