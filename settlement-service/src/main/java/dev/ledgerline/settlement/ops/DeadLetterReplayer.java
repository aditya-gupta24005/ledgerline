package dev.ledgerline.settlement.ops;

import dev.ledgerline.events.Topics;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Republishes a dead letter to the trades topic. Safe to repeat because settlement is idempotent by trade id. */
@Component
class DeadLetterReplayer {

    static final String REPLAYED_FROM_HEADER = "ledgerline-replayed-from";

    private final DeadLetterStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;

    DeadLetterReplayer(DeadLetterStore store, KafkaTemplate<String, String> kafkaTemplate) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
    }

    Optional<ReplayResult> replay(int partition, long offset) {
        return store.find(partition, offset).map(this::republish);
    }

    private ReplayResult republish(ConsumerRecord<String, String> deadLetter) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(Topics.TRADES_EXECUTED, deadLetter.key(), deadLetter.value());
        String origin = Topics.TRADES_EXECUTED_DLT + "/" + deadLetter.partition() + "/" + deadLetter.offset();
        record.headers().add(REPLAYED_FROM_HEADER, origin.getBytes(StandardCharsets.UTF_8));
        try {
            kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while replaying " + origin, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Replay of " + origin + " was not acknowledged", e);
        }
        return new ReplayResult(Topics.TRADES_EXECUTED, deadLetter.partition(), deadLetter.offset());
    }
}
