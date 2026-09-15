package dev.ledgerline.order.engine;

/** Order entry is refused because trades could not be made durable. Only a restart clears it. */
public class EngineHaltedException extends RuntimeException {

    public EngineHaltedException(String message, Throwable cause) {
        super(message, cause);
    }
}
