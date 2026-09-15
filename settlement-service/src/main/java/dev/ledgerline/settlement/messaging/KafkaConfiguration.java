package dev.ledgerline.settlement.messaging;

import dev.ledgerline.events.Topics;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Transient failures are retried with exponential backoff. Messages that can never succeed, and anything
 * still failing after the retries, go to the dead-letter topic on the same partition, instead of blocking
 * every later trade for that symbol.
 */
@Configuration(proxyBeanMethods = false)
class KafkaConfiguration {

    @Bean
    NewTopic tradesExecutedTopic() {
        return TopicBuilder.name(Topics.TRADES_EXECUTED).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic tradesExecutedDeadLetterTopic() {
        return TopicBuilder.name(Topics.TRADES_EXECUTED_DLT).partitions(3).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${ledgerline.settlement.retry.initial-interval:1s}") Duration initialInterval) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(initialInterval.toMillis());
        backOff.setMultiplier(2.0);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(InvalidTradeMessageException.class, IllegalStateException.class);
        return errorHandler;
    }
}
