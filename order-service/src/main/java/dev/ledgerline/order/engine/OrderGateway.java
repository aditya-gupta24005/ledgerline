package dev.ledgerline.order.engine;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.matching.BookSnapshot;
import dev.ledgerline.matching.MatchResult;
import dev.ledgerline.matching.MatchingEngine;
import dev.ledgerline.matching.OrderRequest;
import dev.ledgerline.matching.Trade;
import dev.ledgerline.order.events.TradeEventPublisher;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Runs every engine command on one dedicated thread that owns the {@link MatchingEngine}.
 *
 * <p>Request threads never touch engine state; they hand commands to the sequencer and wait. This
 * single-writer design needs no locks, gives a deterministic order of events, and publishes trades
 * for a symbol in exactly the order they were matched.
 */
@Component
public class OrderGateway implements DisposableBean {

    private static final Duration ENGINE_TIMEOUT = Duration.ofSeconds(5);

    private final MatchingEngine engine = new MatchingEngine();
    private final ExecutorService sequencer =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("matching-engine").factory());

    private final TradeEventPublisher publisher;
    private final Clock clock;
    private final String currency;
    private final String sessionId;

    public OrderGateway(
            TradeEventPublisher publisher, Clock clock, @Value("${ledgerline.currency:USD}") String currency) {
        this.publisher = publisher;
        this.clock = clock;
        this.currency = currency;
        // Engine trade ids restart at 1 on every boot, so qualify them to stay unique downstream.
        this.sessionId = Long.toString(clock.millis(), 36);
    }

    public OrderOutcome submit(OrderRequest request) {
        return onSequencer(() -> {
            MatchResult result = engine.submit(request);
            List<TradeExecuted> trades = result.trades().stream().map(this::toEvent).toList();
            trades.forEach(publisher::publish);
            return new OrderOutcome(result, trades);
        });
    }

    public boolean cancel(String symbol, long orderId) {
        return onSequencer(() -> engine.cancel(symbol, orderId));
    }

    public BookSnapshot book(String symbol, int depth) {
        return onSequencer(() -> engine.snapshot(symbol, depth));
    }

    private TradeExecuted toEvent(Trade trade) {
        return new TradeExecuted(
                "T-" + sessionId + "-" + trade.tradeId(),
                trade.symbol(),
                currency,
                Prices.fromTicks(trade.priceTicks()),
                trade.quantity(),
                trade.buyOrderId(),
                trade.buyAccountId(),
                trade.sellOrderId(),
                trade.sellAccountId(),
                trade.aggressorSide().name(),
                clock.instant());
    }

    private <T> T onSequencer(Callable<T> command) {
        try {
            return sequencer.submit(command).get(ENGINE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the matching engine", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("Matching engine command failed", e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException("Matching engine did not respond within " + ENGINE_TIMEOUT, e);
        }
    }

    @Override
    public void destroy() throws InterruptedException {
        sequencer.shutdown();
        if (!sequencer.awaitTermination(5, TimeUnit.SECONDS)) {
            sequencer.shutdownNow();
        }
    }
}
