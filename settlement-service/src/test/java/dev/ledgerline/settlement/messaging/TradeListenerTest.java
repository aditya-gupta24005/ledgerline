package dev.ledgerline.settlement.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import dev.ledgerline.events.Topics;
import dev.ledgerline.events.TradeExecuted;
import dev.ledgerline.settlement.SettlementService;
import dev.ledgerline.settlement.TradeJson;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.json.JsonMapper;

class TradeListenerTest {

    private final SettlementService settlementService = mock(SettlementService.class);
    private final TradeListener listener = new TradeListener(settlementService, JsonMapper.builder().build());

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>(Topics.TRADES_EXECUTED, 0, 0L, "ACME", value);
    }

    @Test
    void validMessageIsParsedAndSettled() {
        listener.onTradeExecuted(record(TradeJson.valid("T-1")));

        ArgumentCaptor<TradeExecuted> settled = ArgumentCaptor.forClass(TradeExecuted.class);
        verify(settlementService).settle(settled.capture());
        assertThat(settled.getValue().tradeId()).isEqualTo("T-1");
        assertThat(settled.getValue().price()).isEqualByComparingTo("101.5");
        assertThat(settled.getValue().quantity()).isEqualTo(10);
        assertThat(settled.getValue().executedAt()).isEqualTo(Instant.parse("2026-09-15T10:00:00Z"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"bad\":",
            "not json",
            "{}",
            "{\"tradeId\":\"T-2\",\"symbol\":\"ACME\",\"currency\":\"USD\",\"quantity\":10,\"buyOrderId\":1,"
                    + "\"buyAccountId\":\"bob\",\"sellOrderId\":2,\"sellAccountId\":\"alice\","
                    + "\"aggressorSide\":\"BUY\",\"executedAt\":\"2026-09-15T10:00:00Z\"}"
    })
    void invalidMessagesAreRejectedWithoutSettling(String payload) {
        assertThatThrownBy(() -> listener.onTradeExecuted(record(payload)))
                .isInstanceOf(InvalidTradeMessageException.class);

        verifyNoInteractions(settlementService);
    }

    @Test
    void nullPayloadIsRejected() {
        assertThatThrownBy(() -> listener.onTradeExecuted(record(null)))
                .isInstanceOf(InvalidTradeMessageException.class);

        verifyNoInteractions(settlementService);
    }
}
