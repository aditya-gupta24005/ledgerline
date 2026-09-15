package dev.ledgerline.order.engine;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("matchingEngine")
class EngineHealthIndicator implements HealthIndicator {

    private final OrderGateway gateway;

    EngineHealthIndicator(OrderGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public Health health() {
        return gateway.halt()
                .map(halt -> Health.down()
                        .withDetail("reason", halt.reason())
                        .withDetail("haltedAt", halt.haltedAt().toString())
                        .build())
                .orElseGet(() -> Health.up().build());
    }
}
