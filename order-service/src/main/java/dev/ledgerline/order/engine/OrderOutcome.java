package dev.ledgerline.order.engine;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.matching.MatchResult;
import java.util.List;

public record OrderOutcome(MatchResult result, List<TradeExecuted> trades) {
}
