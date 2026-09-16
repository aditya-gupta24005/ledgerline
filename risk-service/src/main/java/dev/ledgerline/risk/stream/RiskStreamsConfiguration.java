package dev.ledgerline.risk.stream;

import dev.ledgerline.events.Topics;
import dev.ledgerline.risk.RiskProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
class RiskStreamsConfiguration {

    /** Declared here as well so the source topic exists before the stream starts, even if order-service never ran. */
    @Bean
    NewTopic tradesExecutedTopic() {
        return TopicBuilder.name(Topics.TRADES_EXECUTED).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic riskAlertsTopic() {
        return TopicBuilder.name(Topics.RISK_ALERTS).partitions(3).replicas(1).build();
    }

    @Bean
    RiskTopology riskTopology(StreamsBuilder streamsBuilder, JsonMapper jsonMapper, RiskProperties properties) {
        RiskTopology topology = new RiskTopology(jsonMapper, properties);
        topology.addTo(streamsBuilder);
        return topology;
    }
}
