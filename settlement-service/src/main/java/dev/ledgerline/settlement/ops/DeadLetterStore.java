package dev.ledgerline.settlement.ops;

import dev.ledgerline.events.Topics;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Reads the dead-letter topic directly with a short-lived, group-less consumer. Nothing is committed, so
 * reading never moves any consumer group's position.
 */
@Component
public class DeadLetterStore {

    private static final Duration POLL_BUDGET = Duration.ofSeconds(5);

    private final ConsumerFactory<String, String> consumerFactory;

    DeadLetterStore(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    /** Up to {@code limit} most recent dead letters across all partitions, newest first. */
    public List<DeadLetterRecord> latest(int limit) {
        try (Consumer<String, String> consumer = newConsumer()) {
            List<TopicPartition> partitions = partitions(consumer);
            consumer.assign(partitions);
            Map<TopicPartition, Long> beginning = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, Math.max(beginning.get(partition), end.get(partition) - limit));
            }

            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            long deadline = System.nanoTime() + POLL_BUDGET.toNanos();
            while (!caughtUp(consumer, partitions, end) && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            records.sort(Comparator.comparingLong((ConsumerRecord<String, String> record) -> record.timestamp()).reversed());
            return records.stream().limit(limit).map(DeadLetterStore::toDeadLetter).toList();
        }
    }

    /** The record at exactly {@code partition}/{@code offset}, or empty if there is none. */
    public Optional<ConsumerRecord<String, String>> find(int partition, long offset) {
        try (Consumer<String, String> consumer = newConsumer()) {
            TopicPartition topicPartition = new TopicPartition(Topics.TRADES_EXECUTED_DLT, partition);
            if (!partitions(consumer).contains(topicPartition)) {
                throw new UnknownDeadLetterPartitionException(partition);
            }
            consumer.assign(List.of(topicPartition));
            long beginning = consumer.beginningOffsets(List.of(topicPartition)).get(topicPartition);
            long end = consumer.endOffsets(List.of(topicPartition)).get(topicPartition);
            if (offset < beginning || offset >= end) {
                return Optional.empty();
            }
            consumer.seek(topicPartition, offset);
            long deadline = System.nanoTime() + POLL_BUDGET.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.offset() == offset) {
                        return Optional.of(record);
                    }
                    if (record.offset() > offset) {
                        return Optional.empty();
                    }
                }
            }
            return Optional.empty();
        }
    }

    private Consumer<String, String> newConsumer() {
        Properties overrides = new Properties();
        overrides.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return consumerFactory.createConsumer(null, "dead-letter-store", null, overrides);
    }

    private static List<TopicPartition> partitions(Consumer<String, String> consumer) {
        return consumer.partitionsFor(Topics.TRADES_EXECUTED_DLT).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();
    }

    private static boolean caughtUp(
            Consumer<String, String> consumer, List<TopicPartition> partitions, Map<TopicPartition, Long> end) {
        return partitions.stream().allMatch(partition -> consumer.position(partition) >= end.get(partition));
    }

    private static DeadLetterRecord toDeadLetter(ConsumerRecord<String, String> record) {
        String causeClass = text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
        return new DeadLetterRecord(
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                causeClass != null ? causeClass : text(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                Instant.ofEpochMilli(record.timestamp()));
    }

    private static String text(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Integer intHeader(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value().length != Integer.BYTES ? null : ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long longHeader(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value().length != Long.BYTES ? null : ByteBuffer.wrap(header.value()).getLong();
    }
}
