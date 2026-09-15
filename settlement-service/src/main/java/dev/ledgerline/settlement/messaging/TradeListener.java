package dev.ledgerline.settlement.messaging;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.settlement.SettlementService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumes trades as plain JSON. The wire contract is the JSON shape plus the {@code eventType} header, so no
 * Java class names cross service boundaries. Anything that cannot be parsed into a valid trade is marked as
 * permanently invalid.
 */
@Component
class TradeListener {

    private final SettlementService settlementService;
    private final JsonMapper jsonMapper;

    TradeListener(SettlementService settlementService, JsonMapper jsonMapper) {
        this.settlementService = settlementService;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = Topics.TRADES_EXECUTED, groupId = "${spring.application.name}")
    void onTradeExecuted(ConsumerRecord<String, String> record) {
        settlementService.settle(parse(record.value()));
    }

    private TradeExecuted parse(String payload) {
        if (payload == null) {
            throw new InvalidTradeMessageException("Empty trade message", null);
        }
        try {
            return jsonMapper.readValue(payload, TradeExecuted.class);
        } catch (JacksonException | IllegalArgumentException | NullPointerException e) {
            throw new InvalidTradeMessageException("Invalid TradeExecuted message: " + e.getMessage(), e);
        }
    }
}
