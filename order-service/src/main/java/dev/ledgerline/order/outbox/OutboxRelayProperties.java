package dev.ledgerline.order.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("ledgerline.outbox.relay")
public record OutboxRelayProperties(
        @DefaultValue("200ms") Duration pollInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("5s") Duration sendTimeout) {
}
