package dev.ledgerline.order;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Tests turn scheduling off and drive the relay by calling {@code OutboxRelay.runOnce()} directly. */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(name = "ledgerline.scheduling.enabled", havingValue = "true", matchIfMissing = true)
class SchedulingConfiguration {
}
