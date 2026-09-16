package dev.ledgerline.risk.query;

import dev.ledgerline.risk.domain.Position;
import dev.ledgerline.risk.stream.RiskTopology;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

@Service
public class PositionQueryService {

    private final RiskStores stores;

    public PositionQueryService(RiskStores stores) {
        this.stores = stores;
    }

    public List<PositionView> positionsFor(String accountId) {
        ReadOnlyKeyValueStore<String, String> lastPrices = stores.lastPrices();
        // The trailing "|" keeps "al" from matching "alice|…".
        String prefix = RiskTopology.positionKey(accountId, "");
        List<PositionView> views = new ArrayList<>();
        try (KeyValueIterator<String, Position> entries = stores.positions().prefixScan(prefix, new StringSerializer())) {
            while (entries.hasNext()) {
                KeyValue<String, Position> entry = entries.next();
                views.add(PositionView.of(entry.value, markPrice(entry.value, lastPrices)));
            }
        }
        views.sort(Comparator.comparing(PositionView::symbol));
        return views;
    }

    private static BigDecimal markPrice(Position position, ReadOnlyKeyValueStore<String, String> lastPrices) {
        String lastTraded = lastPrices.get(position.symbol());
        return lastTraded != null ? new BigDecimal(lastTraded) : position.lastPrice();
    }
}
