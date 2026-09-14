package dev.ledgerline.order.api;

class OrderRejectedException extends RuntimeException {

    private final long orderId;

    OrderRejectedException(long orderId, String reason) {
        super(reason);
        this.orderId = orderId;
    }

    long orderId() {
        return orderId;
    }
}
