package dev.ledgerline.order.events;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes trades keyed by symbol. Sends are asynchronous so a slow broker never stalls the
 * matching thread; failures are logged. Guaranteed delivery needs a transactional outbox, which is
 * on the roadmap.
 */
@Component
class KafkaTradeEventPublisher implements TradeEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTradeEventPublisher.class);

    private final KafkaTemplate<String, TradeExecuted> kafkaTemplate;

    KafkaTradeEventPublisher(KafkaTemplate<String, TradeExecuted> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(TradeExecuted trade) {
        try {
            kafkaTemplate.send(Topics.TRADES_EXECUTED, trade.symbol(), trade)
                    .whenComplete((result, failure) -> {
                        if (failure != null) {
                            log.error("Failed to publish trade {}", trade.tradeId(), failure);
                        }
                    });
        } catch (RuntimeException e) {
            log.error("Failed to publish trade {}", trade.tradeId(), e);
        }
    }
}
