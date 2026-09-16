package dev.ledgerline.risk.query;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledgerline.risk.TopologyFixture;
import dev.ledgerline.risk.domain.Position;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PositionQueryServiceTest {

    private TopologyFixture fixture;
    private PositionQueryService service;

    @BeforeEach
    void setUp(@TempDir Path stateDir) {
        fixture = new TopologyFixture(new BigDecimal("1000000"), stateDir);
        service = new PositionQueryService(new RiskStores() {
            @Override
            public ReadOnlyKeyValueStore<String, Position> positions() {
                return fixture.positions();
            }

            @Override
            public ReadOnlyKeyValueStore<String, String> lastPrices() {
                return fixture.lastPrices();
            }
        });
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void marksEachPositionAtTheSymbolsLastTradedPrice() {
        fixture.pipeTrade("T-1", "alice", "bob", "ACME", 10, "100");
        fixture.pipeTrade("T-2", "carol", "dave", "ACME", 5, "110");
        fixture.pipeTrade("T-3", "alice", "bob", "GLOBX", 4, "20");

        List<PositionView> alice = service.positionsFor("alice");

        assertThat(alice).extracting(PositionView::symbol).containsExactly("ACME", "GLOBX");
        PositionView acme = alice.get(0);
        assertThat(acme.netQuantity()).isEqualTo(10);
        assertThat(acme.averageCost()).isEqualByComparingTo("100");
        assertThat(acme.lastPrice()).as("carol's later trade moved the ACME price").isEqualByComparingTo("110");
        assertThat(acme.notional()).isEqualByComparingTo("1100");
        assertThat(acme.realizedPnl()).isEqualByComparingTo("0");
        assertThat(acme.unrealizedPnl()).isEqualByComparingTo("100");
        assertThat(alice.get(1).unrealizedPnl()).isEqualByComparingTo("0");
    }

    @Test
    void shortPositionsLoseWhenThePriceRises() {
        fixture.pipeTrade("T-1", "alice", "bob", "ACME", 10, "100");
        fixture.pipeTrade("T-2", "carol", "dave", "ACME", 1, "110");

        assertThat(service.positionsFor("bob")).singleElement().satisfies(position -> {
            assertThat(position.netQuantity()).isEqualTo(-10);
            assertThat(position.unrealizedPnl()).isEqualByComparingTo("-100");
        });
    }

    @Test
    void accountIdIsMatchedExactlyNotByPrefix() {
        fixture.pipeTrade("T-1", "alice", "bob", "ACME", 1, "100");

        assertThat(service.positionsFor("al")).isEmpty();
        assertThat(service.positionsFor("nobody")).isEmpty();
    }
}
