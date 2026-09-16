package dev.ledgerline.risk.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import dev.ledgerline.events.RiskAlert;
import dev.ledgerline.risk.TopologyFixture;
import dev.ledgerline.risk.domain.Position;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RiskTopologyTest {

    private TopologyFixture fixture;

    @BeforeEach
    void startTopology(@TempDir Path stateDir) {
        fixture = new TopologyFixture(new BigDecimal("1000"), stateDir);
    }

    @AfterEach
    void stopTopology() {
        fixture.close();
    }

    private Position position(String accountId, String symbol) {
        return fixture.positions().get(RiskTopology.positionKey(accountId, symbol));
    }

    @Test
    void aTradeUpdatesTheBuyerAndSellerPositions() {
        fixture.pipeTrade("T-1", "bob", "alice", "ACME", 5, "100");

        assertThat(position("bob", "ACME").netQuantity()).isEqualTo(5);
        assertThat(position("bob", "ACME").averageCost()).isEqualByComparingTo("100");
        assertThat(position("alice", "ACME").netQuantity()).isEqualTo(-5);
        assertThat(position("alice", "ACME").averageCost()).isEqualByComparingTo("100");
    }

    @Test
    void aRedeliveredTradeIsCountedOnce() {
        fixture.pipeTrade("T-dup", "bob", "alice", "ACME", 5, "100");
        fixture.pipeTrade("T-dup", "bob", "alice", "ACME", 5, "100");

        assertThat(position("bob", "ACME").netQuantity()).isEqualTo(5);
        assertThat(position("alice", "ACME").netQuantity()).isEqualTo(-5);
    }

    @Test
    void tradeIdsAreForgottenAfterTheRetentionPeriod() {
        fixture.pipeTrade("T-old", "bob", "alice", "ACME", 5, "100");

        fixture.advanceWallClock(Duration.ofDays(8));
        fixture.pipeTrade("T-old", "bob", "alice", "ACME", 5, "100");

        assertThat(position("bob", "ACME").netQuantity()).isEqualTo(10);
    }

    @Test
    void crossingTheLimitRaisesOneAlertPerPositionOnlyOnce() {
        fixture.pipeTrade("T-a", "bob", "alice", "ACME", 10, "100");
        assertThat(fixture.readAlerts()).as("notional 1000 is not above the limit").isEmpty();

        fixture.pipeTrade("T-b", "bob", "alice", "ACME", 1, "100");
        fixture.pipeTrade("T-c", "bob", "alice", "ACME", 1, "100");

        assertThat(fixture.readAlerts())
                .extracting(RiskAlert::accountId, RiskAlert::symbol, RiskAlert::netQuantity, RiskAlert::tradeId)
                .containsExactlyInAnyOrder(
                        tuple("bob", "ACME", 11L, "T-b"),
                        tuple("alice", "ACME", -11L, "T-b"));
    }

    @Test
    void alertCarriesTheNotionalAndTheLimit() {
        fixture.pipeTrade("T-big", "bob", "alice", "GLOBX", 2, "600");

        assertThat(fixture.readAlerts()).hasSize(2).allSatisfy(alert -> {
            assertThat(alert.notional()).isEqualByComparingTo("1200");
            assertThat(alert.limit()).isEqualByComparingTo("1000");
        });
    }

    @Test
    void lastPriceIsTheMostRecentTradePerSymbol() {
        fixture.pipeTrade("T-p1", "bob", "alice", "ACME", 1, "100");
        fixture.pipeTrade("T-p2", "carol", "dave", "ACME", 1, "101.5");
        fixture.pipeTrade("T-p3", "bob", "alice", "GLOBX", 1, "20");

        assertThat(new BigDecimal(fixture.lastPrices().get("ACME"))).isEqualByComparingTo("101.5");
        assertThat(new BigDecimal(fixture.lastPrices().get("GLOBX"))).isEqualByComparingTo("20");
    }

    @Test
    void unparseableMessagesAreSkippedWithoutStoppingTheStream() {
        fixture.pipeRaw("{\"bad\":");
        fixture.pipeRaw("not json");
        fixture.pipeTrade("T-after-bad", "bob", "alice", "ACME", 3, "100");

        assertThat(position("bob", "ACME").netQuantity()).isEqualTo(3);
    }
}
