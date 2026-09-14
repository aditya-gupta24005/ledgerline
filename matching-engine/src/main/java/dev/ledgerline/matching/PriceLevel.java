package dev.ledgerline.matching;

/**
 * All resting orders at one price, oldest first, as a doubly linked list threaded through the
 * orders themselves. Appending, taking the head and unlinking an arbitrary order are all O(1).
 */
final class PriceLevel {

    final long priceTicks;

    Order head;
    Order tail;
    long totalQuantity;
    int orderCount;

    PriceLevel(long priceTicks) {
        this.priceTicks = priceTicks;
    }

    void append(Order order) {
        order.level = this;
        order.prev = tail;
        order.next = null;
        if (tail == null) {
            head = order;
        } else {
            tail.next = order;
        }
        tail = order;
        totalQuantity += order.remaining;
        orderCount++;
    }

    void fill(Order order, long quantity) {
        order.remaining -= quantity;
        totalQuantity -= quantity;
    }

    void remove(Order order) {
        if (order.prev == null) {
            head = order.next;
        } else {
            order.prev.next = order.next;
        }
        if (order.next == null) {
            tail = order.prev;
        } else {
            order.next.prev = order.prev;
        }
        totalQuantity -= order.remaining;
        orderCount--;
        order.prev = null;
        order.next = null;
        order.level = null;
    }

    boolean isEmpty() {
        return head == null;
    }
}
