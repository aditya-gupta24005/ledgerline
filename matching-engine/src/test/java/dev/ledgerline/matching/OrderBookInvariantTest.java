package dev.ledgerline.matching;

import static dev.ledgerline.matching.Side.BUY;
import static dev.ledgerline.matching.Side.SELL;
import static org.assertj.core.api.Assertions.assertThat;

import dev.ledgerline.matching.BookSnapshot.LevelView;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Drives the engine with seeded random order flow and checks it against a simple model of which
 * orders should be resting and how much quantity each should have left.
 */
class OrderBookInvariantTest {

    private static final String SYMBOL = "ACME";
    private static final int OPERATIONS = 5_000;

    @ParameterizedTest
    @ValueSource(longs = {1, 7, 42, 2024, 31337})
    void randomOrderFlowKeepsTheBookConsistent(long seed) {
        Random random = new Random(seed);
        MatchingEngine engine = new MatchingEngine();
        Map<Long, Long> expectedResting = new HashMap<>();
        List<Long> submittedIds = new ArrayList<>();

        for (int i = 0; i < OPERATIONS; i++) {
            if (!submittedIds.isEmpty() && random.nextInt(10) == 0) {
                long orderId = submittedIds.remove(random.nextInt(submittedIds.size()));
                boolean shouldBeResting = expectedResting.remove(orderId) != null;
                assertThat(engine.cancel(SYMBOL, orderId, "acct")).isEqualTo(shouldBeResting);
            } else {
                submitRandomOrder(random, engine, expectedResting, submittedIds);
            }
            assertBookMatchesModel(engine, expectedResting);
        }
    }

    private static void submitRandomOrder(
            Random random, MatchingEngine engine, Map<Long, Long> expectedResting, List<Long> submittedIds) {
        Side side = random.nextBoolean() ? BUY : SELL;
        long quantity = 1 + random.nextInt(100);
        OrderRequest request = random.nextInt(5) == 0
                ? OrderRequest.market("acct", SYMBOL, side, quantity)
                : OrderRequest.limit("acct", SYMBOL, side, 9_900 + random.nextInt(200), quantity);

        MatchResult result = engine.submit(request);

        long traded = 0;
        for (Trade trade : result.trades()) {
            traded += trade.quantity();
            long restingOrderId = side == BUY ? trade.sellOrderId() : trade.buyOrderId();
            long left = expectedResting.merge(restingOrderId, -trade.quantity(), Long::sum);
            assertThat(left).as("resting order %d overfilled", restingOrderId).isNotNegative();
            if (left == 0) {
                expectedResting.remove(restingOrderId);
            }
            if (request.type() == OrderType.LIMIT && side == BUY) {
                assertThat(trade.priceTicks()).isLessThanOrEqualTo(request.priceTicks());
            } else if (request.type() == OrderType.LIMIT) {
                assertThat(trade.priceTicks()).isGreaterThanOrEqualTo(request.priceTicks());
            }
        }

        assertThat(traded).isEqualTo(result.filledQuantity());
        if (request.type() == OrderType.LIMIT) {
            assertThat(result.filledQuantity() + result.restingQuantity()).isEqualTo(quantity);
        } else {
            assertThat(result.restingQuantity()).isZero();
        }
        if (result.restingQuantity() > 0) {
            expectedResting.put(result.orderId(), result.restingQuantity());
        }
        submittedIds.add(result.orderId());
    }

    private static void assertBookMatchesModel(MatchingEngine engine, Map<Long, Long> expectedResting) {
        OptionalLong bestBid = engine.bestBid(SYMBOL);
        OptionalLong bestAsk = engine.bestAsk(SYMBOL);
        if (bestBid.isPresent() && bestAsk.isPresent()) {
            assertThat(bestBid.getAsLong()).as("book is crossed").isLessThan(bestAsk.getAsLong());
        }

        BookSnapshot book = engine.snapshot(SYMBOL, Integer.MAX_VALUE);
        assertThat(book.bids()).extracting(LevelView::priceTicks).isSortedAccordingTo(Comparator.reverseOrder());
        assertThat(book.asks()).extracting(LevelView::priceTicks).isSorted();

        long quantityOnBook = Stream.concat(book.bids().stream(), book.asks().stream())
                .mapToLong(LevelView::quantity)
                .sum();
        int ordersOnBook = Stream.concat(book.bids().stream(), book.asks().stream())
                .mapToInt(LevelView::orderCount)
                .sum();
        assertThat(quantityOnBook).isEqualTo(expectedResting.values().stream().mapToLong(Long::longValue).sum());
        assertThat(ordersOnBook).isEqualTo(expectedResting.size());
    }
}
