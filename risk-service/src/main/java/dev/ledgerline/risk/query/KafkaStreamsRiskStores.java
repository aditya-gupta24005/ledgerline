package dev.ledgerline.risk.query;

import dev.ledgerline.risk.domain.Position;
import dev.ledgerline.risk.stream.RiskTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/** Interactive queries against this instance's local stores. Single instance only; see README. */
@Component
class KafkaStreamsRiskStores implements RiskStores {

    private final StreamsBuilderFactoryBean streamsFactory;

    KafkaStreamsRiskStores(StreamsBuilderFactoryBean streamsFactory) {
        this.streamsFactory = streamsFactory;
    }

    @Override
    public ReadOnlyKeyValueStore<String, Position> positions() {
        return store(RiskTopology.POSITIONS_STORE);
    }

    @Override
    public ReadOnlyKeyValueStore<String, String> lastPrices() {
        return store(RiskTopology.LAST_PRICES_STORE);
    }

    private <V> ReadOnlyKeyValueStore<String, V> store(String name) {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            throw new RiskDataUnavailableException(
                    "Risk data is not available yet (stream state: " + (streams == null ? "NOT_STARTED" : streams.state()) + ")");
        }
        try {
            return streams.store(StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.<String, V>keyValueStore()));
        } catch (InvalidStateStoreException e) {
            throw new RiskDataUnavailableException("Risk data is not available yet: " + e.getMessage());
        }
    }
}
