package dev.ledgerline.matching;

import static dev.ledgerline.matching.Side.BUY;
import static dev.ledgerline.matching.Side.SELL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import dev.ledgerline.matching.BookSnapshot.LevelView;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MatchingEngineTest {

    private static final String SYMBOL = "ACME";

    private final MatchingEngine engine = new MatchingEngine();

    private MatchResult limit(String account, Side side, long priceTicks, long quantity) {
        return engine.submit(OrderRequest.limit(account, SYMBOL, side, priceTicks, quantity));
    }

    private MatchResult market(String account, Side side, long quantity) {
        return engine.submit(OrderRequest.market(account, SYMBOL, side, quantity));
    }

    private BookSnapshot book() {
        return engine.snapshot(SYMBOL, 10);
    }

    @Nested
    class LimitOrders {

        @Test
        void restsOnEmptyBook() {
            MatchResult result = limit("alice", BUY, 100_00, 10);

            assertThat(result.status()).isEqualTo(OrderStatus.NEW);
            assertThat(result.trades()).isEmpty();
            assertThat(result.restingQuantity()).isEqualTo(10);
            assertThat(engine.bestBid(SYMBOL)).hasValue(100_00);
            assertThat(engine.bestAsk(SYMBOL)).isEmpty();
        }

        @Test
        void doesNotTradeWhenPricesDoNotCross() {
            limit("seller", SELL, 101_00, 5);
            MatchResult buy = limit("buyer", BUY, 100_00, 5);

            assertThat(buy.status()).isEqualTo(OrderStatus.NEW);
            assertThat(engine.bestBid(SYMBOL)).hasValue(100_00);
            assertThat(engine.bestAsk(SYMBOL)).hasValue(101_00);
        }

        @Test
        void tradesAtTheRestingOrdersPrice() {
            MatchResult sell = limit("seller", SELL, 100_00, 10);
            MatchResult buy = limit("buyer", BUY, 102_00, 10);

            assertThat(buy.status()).isEqualTo(OrderStatus.FILLED);
            assertThat(buy.filledQuantity()).isEqualTo(10);
            assertThat(buy.trades()).singleElement().satisfies(trade -> {
                assertThat(trade.priceTicks()).isEqualTo(100_00);
                assertThat(trade.quantity()).isEqualTo(10);
                assertThat(trade.buyOrderId()).isEqualTo(buy.orderId());
                assertThat(trade.buyAccountId()).isEqualTo("buyer");
                assertThat(trade.sellOrderId()).isEqualTo(sell.orderId());
                assertThat(trade.sellAccountId()).isEqualTo("seller");
                assertThat(trade.aggressorSide()).isEqualTo(BUY);
            });
            assertThat(book().bids()).isEmpty();
            assertThat(book().asks()).isEmpty();
        }

        @Test
        void partialFillLeavesRemainderResting() {
            limit("seller", SELL, 100_00, 4);
            MatchResult buy = limit("buyer", BUY, 100_00, 10);

            assertThat(buy.status()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
            assertThat(buy.filledQuantity()).isEqualTo(4);
            assertThat(buy.restingQuantity()).isEqualTo(6);
            assertThat(book().bids()).containsExactly(new LevelView(100_00, 6, 1));
            assertThat(book().asks()).isEmpty();
        }

        @Test
        void sweepsLevelsBestPriceFirstAndStopsAtLimit() {
            limit("s1", SELL, 101_00, 5);
            limit("s2", SELL, 100_00, 5);
            limit("s3", SELL, 102_00, 5);

            MatchResult buy = limit("buyer", BUY, 101_00, 12);

            assertThat(buy.trades())
                    .extracting(Trade::priceTicks, Trade::quantity)
                    .containsExactly(tuple(100_00L, 5L), tuple(101_00L, 5L));
            assertThat(buy.trades()).extracting(Trade::tradeId).doesNotHaveDuplicates().isSorted();
            assertThat(buy.status()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
            assertThat(book().bids()).containsExactly(new LevelView(101_00, 2, 1));
            assertThat(book().asks()).containsExactly(new LevelView(102_00, 5, 1));
        }

        @Test
        void honoursTimePriorityWithinAPriceLevel() {
            MatchResult early = limit("early", SELL, 100_00, 5);
            MatchResult late = limit("late", SELL, 100_00, 5);

            MatchResult buy = limit("buyer", BUY, 100_00, 7);

            assertThat(buy.trades())
                    .extracting(Trade::sellOrderId, Trade::quantity)
                    .containsExactly(tuple(early.orderId(), 5L), tuple(late.orderId(), 2L));
            assertThat(book().asks()).containsExactly(new LevelView(100_00, 3, 1));
        }
    }

    @Nested
    class MarketOrders {

        @Test
        void consumesLiquidityAtAnyPrice() {
            limit("s1", SELL, 100_00, 3);
            limit("s2", SELL, 150_00, 3);

            MatchResult buy = market("buyer", BUY, 6);

            assertThat(buy.status()).isEqualTo(OrderStatus.FILLED);
            assertThat(buy.trades()).extracting(Trade::priceTicks).containsExactly(100_00L, 150_00L);
        }

        @Test
        void cancelsUnfilledRemainderInsteadOfResting() {
            limit("seller", SELL, 100_00, 3);

            MatchResult buy = market("buyer", BUY, 5);

            assertThat(buy.status()).isEqualTo(OrderStatus.CANCELLED);
            assertThat(buy.filledQuantity()).isEqualTo(3);
            assertThat(buy.restingQuantity()).isZero();
            assertThat(book().bids()).isEmpty();
        }

        @Test
        void isCancelledOnEmptyBook() {
            MatchResult sell = market("seller", SELL, 5);

            assertThat(sell.status()).isEqualTo(OrderStatus.CANCELLED);
            assertThat(sell.trades()).isEmpty();
            assertThat(sell.filledQuantity()).isZero();
        }
    }

    @Nested
    class Cancellation {

        @Test
        void removesOrderFromMiddleOfQueueAndKeepsPriorityOfOthers() {
            MatchResult first = limit("a", SELL, 100_00, 1);
            MatchResult middle = limit("b", SELL, 100_00, 2);
            MatchResult last = limit("c", SELL, 100_00, 3);

            assertThat(engine.cancel(SYMBOL, middle.orderId(), "b")).isTrue();
            assertThat(book().asks()).containsExactly(new LevelView(100_00, 4, 2));

            MatchResult buy = limit("buyer", BUY, 100_00, 4);
            assertThat(buy.trades())
                    .extracting(Trade::sellOrderId)
                    .containsExactly(first.orderId(), last.orderId());
        }

        @Test
        void removesPriceLevelOnceEmpty() {
            MatchResult bid = limit("alice", BUY, 99_00, 1);

            assertThat(engine.cancel(SYMBOL, bid.orderId(), "alice")).isTrue();
            assertThat(engine.bestBid(SYMBOL)).isEmpty();
        }

        @Test
        void returnsFalseForFilledOrUnknownOrders() {
            MatchResult sell = limit("seller", SELL, 100_00, 1);
            limit("buyer", BUY, 100_00, 1);

            assertThat(engine.cancel(SYMBOL, sell.orderId(), "seller")).isFalse();
            assertThat(engine.cancel(SYMBOL, 999, "seller")).isFalse();
            assertThat(engine.cancel("UNKNOWN", 1, "seller")).isFalse();
        }

        @Test
        void anotherAccountCannotCancelTheOrderAndItStaysOnTheBook() {
            MatchResult bid = limit("alice", BUY, 99_00, 7);

            assertThat(engine.cancel(SYMBOL, bid.orderId(), "mallory")).isFalse();

            assertThat(book().bids()).containsExactly(new LevelView(99_00, 7, 1));
            assertThat(engine.cancel(SYMBOL, bid.orderId(), "alice")).isTrue();
        }
    }

    static Stream<Arguments> invalidOrders() {
        return Stream.of(
                arguments(OrderRequest.limit("a", SYMBOL, BUY, 100_00, 0), "quantity must be positive"),
                arguments(OrderRequest.limit("a", SYMBOL, BUY, 0, 10), "limit price must be positive"),
                arguments(new OrderRequest("a", SYMBOL, SELL, OrderType.MARKET, 100_00, 10),
                        "market orders must not carry a price"),
                arguments(OrderRequest.limit(" ", SYMBOL, BUY, 100_00, 10), "accountId is required"),
                arguments(OrderRequest.limit("a", "", BUY, 100_00, 10), "symbol is required"));
    }

    @ParameterizedTest
    @MethodSource("invalidOrders")
    void rejectsInvalidOrdersWithoutTouchingTheBook(OrderRequest request, String expectedReason) {
        MatchResult result = engine.submit(request);

        assertThat(result.status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.rejectReason()).isEqualTo(expectedReason);
        assertThat(book().bids()).isEmpty();
        assertThat(book().asks()).isEmpty();
    }
}
