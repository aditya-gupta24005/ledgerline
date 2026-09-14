package dev.ledgerline.settlement.domain;

import dev.ledgerline.events.TradeExecuted;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Double-entry postings for a trade: the buyer pays cash and receives securities, the seller does
 * the opposite. For every asset the postings sum to zero, so value is never created or lost.
 */
public final class TradePostings {

    private TradePostings() {
    }

    public static List<Posting> of(TradeExecuted trade) {
        BigDecimal quantity = BigDecimal.valueOf(trade.quantity());
        BigDecimal notional = trade.price().multiply(quantity);
        List<Posting> postings = List.of(
                new Posting(trade.buyAccountId(), trade.currency(), notional.negate()),
                new Posting(trade.sellAccountId(), trade.currency(), notional),
                new Posting(trade.buyAccountId(), trade.symbol(), quantity),
                new Posting(trade.sellAccountId(), trade.symbol(), quantity.negate()));
        assertBalanced(postings);
        return postings;
    }

    static void assertBalanced(List<Posting> postings) {
        Map<String, BigDecimal> netByAsset = postings.stream()
                .collect(Collectors.groupingBy(Posting::asset,
                        Collectors.reducing(BigDecimal.ZERO, Posting::amount, BigDecimal::add)));
        netByAsset.forEach((asset, net) -> {
            if (net.signum() != 0) {
                throw new IllegalStateException("Postings for " + asset + " do not balance: net " + net);
            }
        });
    }
}
