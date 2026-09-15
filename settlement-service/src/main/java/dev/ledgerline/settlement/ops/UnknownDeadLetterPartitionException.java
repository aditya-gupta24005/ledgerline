package dev.ledgerline.settlement.ops;

public class UnknownDeadLetterPartitionException extends RuntimeException {

    public UnknownDeadLetterPartitionException(int partition) {
        super("Dead-letter topic has no partition " + partition);
    }
}
