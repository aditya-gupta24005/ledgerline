package dev.ledgerline.risk;

import java.math.BigDecimal;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param positionNotionalLimit an alert is raised the first time a position's notional goes above this
 * @param dedupeRetention       how long processed trade ids are remembered for de-duplication
 */
@ConfigurationProperties("ledgerline.risk")
public record RiskProperties(
        @DefaultValue("1000000") BigDecimal positionNotionalLimit,
        @DefaultValue("7d") Duration dedupeRetention) {
}
