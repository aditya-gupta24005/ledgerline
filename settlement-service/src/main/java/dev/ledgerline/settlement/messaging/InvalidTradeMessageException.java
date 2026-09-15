package dev.ledgerline.settlement.messaging;

/** The message can never be processed, so retrying is pointless and it goes straight to the dead-letter topic. */
public class InvalidTradeMessageException extends RuntimeException {

    public InvalidTradeMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
