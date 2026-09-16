package dev.ledgerline.risk.stream;

import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

/** Plain-JSON serdes: no type headers are written or read, so no Java class names go over the wire. */
final class JsonSerdes {

    private JsonSerdes() {
    }

    static <T> Serde<T> of(Class<T> type, JsonMapper jsonMapper) {
        return new JacksonJsonSerde<T>(type, jsonMapper).noTypeInfo().ignoreTypeHeaders();
    }
}
