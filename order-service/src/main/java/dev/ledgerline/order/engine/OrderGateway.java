package dev.ledgerline.order.engine;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.matching.BookSnapshot;
import dev.ledgerline.matching.MatchResult;
import dev.ledgerline.matching.MatchingEngine;
import dev.ledgerline.matching.OrderRequest;
import dev.ledgerline.matching.Trade;
import dev.ledgerline.order.outbox.OutboxWriter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Runs every engine command on one dedicated thread that owns the {@link MatchingEngine}.
 *
 * <p>Request threads never touch engine state; they hand commands to the sequencer and wait. This
 * single-writer design needs no locks and gives a deterministic order of events. A command's trades are
 * written to the outbox before the caller sees them. If that write fails, the in-memory book already
 * contains trades that are not durable, so order entry halts until restart instead of continuing on an
 * unrecorded state.
 */
@Component
public class OrderGateway implements DisposableBean {

    public record Halt(String reason, Instant haltedAt) {
    }

    static final String HALTED_DETAIL = "Order entry halted after a persistence failure; restart required";

    private static final Logger log = LoggerFactory.getLogger(OrderGateway.class);
    private static final Duration ENGINE_TIMEOUT = Duration.ofSeconds(5);

    private final MatchingEngine engine = new MatchingEngine();
    private final ExecutorService sequencer =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("matching-engine").factory());

    private final OutboxWriter outboxWriter;
    private final Clock clock;
    private final String currency;
    private final String sessionId;

    private volatile Halt halt;

    public OrderGateway(
            OutboxWriter outboxWriter, Clock clock, @Value("${ledgerline.currency:USD}") String currency) {
        this.outboxWriter = outboxWriter;
        this.clock = clock;
        this.currency = currency;
        // Engine trade ids restart at 1 on every boot, so qualify them to stay unique downstream.
        this.sessionId = Long.toString(clock.millis(), 36);
    }

    public OrderOutcome submit(OrderRequest request) {
        return onSequencer(() -> {
            ensureRunning();
            MatchResult result = engine.submit(request);
            List<TradeExecuted> trades = result.trades().stream().map(this::toEvent).toList();
            persist(trades);
            return new OrderOutcome(result, trades);
        });
    }

    public boolean cancel(String symbol, long orderId) {
        return onSequencer(() -> {
            ensureRunning();
            return engine.cancel(symbol, orderId);
        });
    }

    public BookSnapshot book(String symbol, int depth) {
        return onSequencer(() -> engine.snapshot(symbol, depth));
    }

    public Optional<Halt> halt() {
        return Optional.ofNullable(halt);
    }

    private void persist(List<TradeExecuted> trades) {
        if (trades.isEmpty()) {
            return;
        }
        try {
            outboxWriter.append(trades);
        } catch (RuntimeException e) {
            String reason = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            halt = new Halt(reason, clock.instant());
            log.error("Halting order entry: {} trade(s) could not be written to the outbox", trades.size(), e);
            throw new EngineHaltedException(HALTED_DETAIL, e);
        }
    }

    private void ensureRunning() {
        if (halt != null) {
            throw new EngineHaltedException(HALTED_DETAIL, null);
        }
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
