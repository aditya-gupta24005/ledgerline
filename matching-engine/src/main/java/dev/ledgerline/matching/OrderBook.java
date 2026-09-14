package dev.ledgerline.matching;

import dev.ledgerline.matching.BookSnapshot.LevelView;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/**
 * Limit order book for a single symbol with price-time priority.
 *
 * <p>Price levels live in sorted maps with the best price first. Orders within a level form an
 * intrusive linked list, and an id index points straight at each resting order, so cancelling is
 * O(log levels) to drop an emptied level and O(1) otherwise.
 */
final class OrderBook {

    private final String symbol;
    private final NavigableMap<Long, PriceLevel> bids = new TreeMap<>(Comparator.reverseOrder());
    private final NavigableMap<Long, PriceLevel> asks = new TreeMap<>();
    private final Map<Long, Order> restingOrders = new HashMap<>();

    OrderBook(String symbol) {
        this.symbol = symbol;
    }

    MatchResult process(Order incoming, OrderType type, LongSupplier tradeIds) {
        NavigableMap<Long, PriceLevel> opposite = incoming.side == Side.BUY ? asks : bids;
        List<Trade> trades = new ArrayList<>();

        while (incoming.remaining > 0 && !opposite.isEmpty()) {
            PriceLevel best = opposite.firstEntry().getValue();
            if (type == OrderType.LIMIT && !crosses(incoming, best.priceTicks)) {
                break;
            }
            Order resting = best.head;
            long quantity = Math.min(incoming.remaining, resting.remaining);
            incoming.remaining -= quantity;
            best.fill(resting, quantity);
            trades.add(trade(tradeIds.getAsLong(), incoming, resting, best.priceTicks, quantity));

            if (resting.remaining == 0) {
                best.remove(resting);
                restingOrders.remove(resting.orderId);
                if (best.isEmpty()) {
                    opposite.pollFirstEntry();
                }
            }
        }

        long filled = incoming.quantity - incoming.remaining;
        if (incoming.remaining == 0) {
            return new MatchResult(incoming.orderId, OrderStatus.FILLED, filled, 0, trades, null);
        }
        if (type == OrderType.MARKET) {
            return new MatchResult(incoming.orderId, OrderStatus.CANCELLED, filled, 0, trades, null);
        }
        rest(incoming);
        OrderStatus status = filled == 0 ? OrderStatus.NEW : OrderStatus.PARTIALLY_FILLED;
        return new MatchResult(incoming.orderId, status, filled, incoming.remaining, trades, null);
    }

    boolean cancel(long orderId) {
        Order order = restingOrders.remove(orderId);
        if (order == null) {
            return false;
        }
        PriceLevel level = order.level;
        level.remove(order);
        if (level.isEmpty()) {
            levelsFor(order.side).remove(level.priceTicks);
        }
        return true;
    }

    OptionalLong bestBid() {
        return bids.isEmpty() ? OptionalLong.empty() : OptionalLong.of(bids.firstKey());
    }

    OptionalLong bestAsk() {
        return asks.isEmpty() ? OptionalLong.empty() : OptionalLong.of(asks.firstKey());
    }

    BookSnapshot snapshot(int depth) {
        return new BookSnapshot(symbol, view(bids, depth), view(asks, depth));
    }

    private void rest(Order order) {
        levelsFor(order.side).computeIfAbsent(order.priceTicks, PriceLevel::new).append(order);
        restingOrders.put(order.orderId, order);
    }

    private NavigableMap<Long, PriceLevel> levelsFor(Side side) {
        return side == Side.BUY ? bids : asks;
    }

    private static boolean crosses(Order incoming, long restingPrice) {
        return incoming.side == Side.BUY
                ? incoming.priceTicks >= restingPrice
                : incoming.priceTicks <= restingPrice;
    }

    private Trade trade(long tradeId, Order incoming, Order resting, long priceTicks, long quantity) {
        Order buy = incoming.side == Side.BUY ? incoming : resting;
        Order sell = incoming.side == Side.BUY ? resting : incoming;
        return new Trade(tradeId, symbol, priceTicks, quantity,
                buy.orderId, buy.accountId, sell.orderId, sell.accountId, incoming.side);
    }

    private static List<LevelView> view(NavigableMap<Long, PriceLevel> levels, int depth) {
        return levels.values().stream()
                .limit(depth)
                .map(level -> new LevelView(level.priceTicks, level.totalQuantity, level.orderCount))
                .toList();
    }
}
