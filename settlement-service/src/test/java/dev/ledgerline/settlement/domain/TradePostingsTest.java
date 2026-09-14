package dev.ledgerline.settlement.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.ledgerline.events.TradeExecuted;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TradePostingsTest {

    private static TradeExecuted trade(String buyer, String seller) {
        return new TradeExecuted("T-1", "ACME", "USD", new BigDecimal("101.2500"), 40,
                1, buyer, 2, seller, "BUY", Instant.parse("2026-09-14T10:15:30Z"));
    }

    @Test
    void buyerPaysCashAndReceivesSecuritiesSellerDoesTheOpposite() {
        assertThat(TradePostings.of(trade("alice", "bob"))).containsExactlyInAnyOrder(
                new Posting("alice", "USD", new BigDecimal("-4050.0000")),
                new Posting("bob", "USD", new BigDecimal("4050.0000")),
                new Posting("alice", "ACME", new BigDecimal("40")),
                new Posting("bob", "ACME", new BigDecimal("-40")));
    }

    @Test
    void selfTradeNetsToZeroForTheAccount() {
        Map<String, BigDecimal> netByAsset = TradePostings.of(trade("alice", "alice")).stream()
                .collect(Collectors.groupingBy(Posting::asset,
                        Collectors.reducing(BigDecimal.ZERO, Posting::amount, BigDecimal::add)));

        assertThat(netByAsset.values()).allSatisfy(net -> assertThat(net).isZero());
    }

    @Test
    void rejectsUnbalancedPostings() {
        List<Posting> unbalanced = List.of(
                new Posting("alice", "USD", new BigDecimal("-10")),
                new Posting("bob", "USD", new BigDecimal("9.99")));

        assertThatThrownBy(() -> TradePostings.assertBalanced(unbalanced))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("USD");
    }
}
