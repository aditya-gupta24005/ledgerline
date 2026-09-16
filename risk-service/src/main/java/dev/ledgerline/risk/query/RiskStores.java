package dev.ledgerline.risk.query;

import dev.ledgerline.risk.domain.Position;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** Read access to the risk state stores. Throws {@link RiskDataUnavailableException} while they are not queryable. */
public interface RiskStores {

    ReadOnlyKeyValueStore<String, Position> positions();

    ReadOnlyKeyValueStore<String, String> lastPrices();
}
