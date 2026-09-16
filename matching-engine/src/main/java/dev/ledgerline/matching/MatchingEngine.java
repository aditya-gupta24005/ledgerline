package dev.ledgerline.matching;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Entry point to the matching engine: validates orders, assigns ids and routes them to per-symbol
 * books.
 *
 * <p>Deliberately not thread-safe. As in LMAX-style exchange designs, one thread sequences every
 * command, which keeps matching deterministic and free of locks. Callers that accept orders from
 * many threads must hand them to that single thread (see the order service's gateway).
 */
public final class MatchingEngine {

    private final Map<String, OrderBook> books = new HashMap<>();
    private long nextOrderId = 1;
    private long nextTradeId = 1;

    public MatchResult submit(OrderRequest request) {
        Objects.requireNonNull(request, "request");
        long orderId = nextOrderId++;
        String rejectReason = validate(request);
        if (rejectReason != null) {
            return MatchResult.rejected(orderId, rejectReason);
        }
        Order order = new Order(orderId, request.accountId(), request.side(), request.priceTicks(), request.quantity());
        return books.computeIfAbsent(request.symbol(), OrderBook::new)
                .process(order, request.type(), this::allocateTradeId);
    }

    /** Cancels a resting order. Returns false if it is unknown, no longer resting, or owned by another account. */
    public boolean cancel(String symbol, long orderId, String accountId) {
        OrderBook book = books.get(symbol);
        return book != null && book.cancel(orderId, accountId);
    }

    public OptionalLong bestBid(String symbol) {
        OrderBook book = books.get(symbol);
        return book == null ? OptionalLong.empty() : book.bestBid();
    }

    public OptionalLong bestAsk(String symbol) {
        OrderBook book = books.get(symbol);
        return book == null ? OptionalLong.empty() : book.bestAsk();
    }

    public BookSnapshot snapshot(String symbol, int depth) {
        if (depth < 0) {
            throw new IllegalArgumentException("depth must not be negative");
        }
        OrderBook book = books.get(symbol);
        return book == null ? new BookSnapshot(symbol, List.of(), List.of()) : book.snapshot(depth);
    }

    private long allocateTradeId() {
        return nextTradeId++;
    }

    private static String validate(OrderRequest request) {
        if (request.symbol() == null || request.symbol().isBlank()) {
            return "symbol is required";
        }
        if (request.accountId() == null || request.accountId().isBlank()) {
            return "accountId is required";
        }
        if (request.side() == null) {
            return "side is required";
        }
        if (request.type() == null) {
            return "type is required";
        }
        if (request.quantity() <= 0) {
            return "quantity must be positive";
        }
        if (request.type() == OrderType.LIMIT && request.priceTicks() <= 0) {
            return "limit price must be positive";
        }
        if (request.type() == OrderType.MARKET && request.priceTicks() != 0) {
            return "market orders must not carry a price";
        }
        return null;
    }
}
