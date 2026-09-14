package dev.ledgerline.settlement.messaging;

import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.events.Topics;
import dev.ledgerline.settlement.SettlementService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class TradeListener {

    private final SettlementService settlementService;

    TradeListener(SettlementService settlementService) {
        this.settlementService = settlementService;
    }

    @KafkaListener(topics = Topics.TRADES_EXECUTED, groupId = "${spring.application.name}")
    void onTradeExecuted(TradeExecuted trade) {
        settlementService.settle(trade);
    }
}
