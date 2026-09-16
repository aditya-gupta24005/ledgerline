package dev.ledgerline.risk.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PositionTest {

    private static final BigDecimal NO_LIMIT = new BigDecimal("1000000000");

    private static Fill fill(long signedQuantity, String price) {
        return new Fill("T-" + signedQuantity + "@" + price, "alice", "ACME", signedQuantity,
                new BigDecimal(price), Instant.parse("2026-09-16T10:00:00Z"));
    }

    private static Position positionAfter(Fill... fills) {
        Position position = Position.empty();
        for (Fill fill : fills) {
            position = position.apply(fill, NO_LIMIT);
        }
        return position;
    }

    @Test
    void openingALongSetsAverageCostToTheFillPrice() {
        Position position = positionAfter(fill(10, "100"));

        assertThat(position.accountId()).isEqualTo("alice");
        assertThat(position.symbol()).isEqualTo("ACME");
        assertThat(position.netQuantity()).isEqualTo(10);
        assertThat(position.averageCost()).isEqualByComparingTo("100");
        assertThat(position.realizedPnl()).isEqualByComparingTo("0");
        assertThat(position.lastPrice()).isEqualByComparingTo("100");
        assertThat(position.notional()).isEqualByComparingTo("1000");
        assertThat(position.lastTradeId()).isEqualTo("T-10@100");
    }

    @Test
    void addingToALongWeightsTheAverageCost() {
        Position position = positionAfter(fill(10, "100"), fill(10, "110"));

        assertThat(position.netQuantity()).isEqualTo(20);
        assertThat(position.averageCost()).isEqualByComparingTo("105");
        assertThat(position.realizedPnl()).isEqualByComparingTo("0");
    }

    @Test
    void reducingALongRealisesProfitAgainstAverageCost() {
        Position position = positionAfter(fill(20, "105"), fill(-5, "120"));

        assertThat(position.netQuantity()).isEqualTo(15);
        assertThat(position.averageCost()).isEqualByComparingTo("105");
        assertThat(position.realizedPnl()).isEqualByComparingTo("75");
    }

    @Test
    void reducingAShortRealisesALossWhenThePriceRose() {
        Position position = positionAfter(fill(-10, "50"), fill(4, "55"));

        assertThat(position.netQuantity()).isEqualTo(-6);
        assertThat(position.averageCost()).isEqualByComparingTo("50");
        assertThat(position.realizedPnl()).isEqualByComparingTo("-20");
    }

    @Test
    void crossingThroughZeroClosesTheOldPositionAndOpensTheRestAtTheFillPrice() {
        Position position = positionAfter(fill(10, "100"), fill(-15, "90"));

        assertThat(position.netQuantity()).isEqualTo(-5);
        assertThat(position.averageCost()).isEqualByComparingTo("90");
        assertThat(position.realizedPnl()).isEqualByComparingTo("-100");
        assertThat(position.notional()).isEqualByComparingTo("450");
    }

    @Test
    void closingFlatResetsAverageCostAndKeepsRealisedPnl() {
        Position position = positionAfter(fill(10, "100"), fill(-10, "101"));

        assertThat(position.netQuantity()).isZero();
        assertThat(position.averageCost()).isEqualByComparingTo("0");
        assertThat(position.realizedPnl()).isEqualByComparingTo("10");
        assertThat(position.notional()).isEqualByComparingTo("0");
    }

    @Test
    void flagsTheLimitOnlyOnTheFillThatCrossesIt() {
        BigDecimal limit = new BigDecimal("1000");

        Position atLimit = Position.empty().apply(fill(10, "100"), limit);
        Position crossed = atLimit.apply(fill(1, "100"), limit);
        Position stillAbove = crossed.apply(fill(1, "100"), limit);
        Position backBelow = stillAbove.apply(fill(-5, "100"), limit);
        Position crossedAgain = backBelow.apply(fill(5, "100"), limit);

        assertThat(atLimit.limitBreachedByLastFill()).as("notional 1000 is not above 1000").isFalse();
        assertThat(crossed.limitBreachedByLastFill()).as("1000 -> 1100").isTrue();
        assertThat(stillAbove.limitBreachedByLastFill()).as("1100 -> 1200").isFalse();
        assertThat(backBelow.limitBreachedByLastFill()).as("1200 -> 700").isFalse();
        assertThat(crossedAgain.limitBreachedByLastFill()).as("700 -> 1200").isTrue();
    }

    @Test
    void unrealisedPnlMarksLongsAndShortsAgainstAverageCost() {
        assertThat(positionAfter(fill(10, "100")).unrealizedPnl(new BigDecimal("103.5"))).isEqualByComparingTo("35");
        assertThat(positionAfter(fill(-10, "50")).unrealizedPnl(new BigDecimal("45"))).isEqualByComparingTo("50");
    }
}
