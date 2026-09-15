package dev.ledgerline.settlement.ops;

/** Identifies the dead-letter record ({@code partition}, {@code offset}) that was republished to {@code replayedTo}. */
public record ReplayResult(String replayedTo, int partition, long offset) {
}
