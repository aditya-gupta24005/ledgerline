package dev.ledgerline.settlement.ops;

import java.time.Instant;

public record DeadLetterRecord(
        int partition,
        long offset,
        String key,
        String payload,
        String exceptionClass,
        String exceptionMessage,
        Integer originalPartition,
        Long originalOffset,
        Instant failedAt) {
}
