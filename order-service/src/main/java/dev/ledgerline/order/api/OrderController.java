package dev.ledgerline.order.api;

import dev.ledgerline.matching.MatchResult;
import dev.ledgerline.matching.OrderStatus;
import dev.ledgerline.order.engine.OrderGateway;
import dev.ledgerline.order.engine.OrderOutcome;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
class OrderController {

    private static final int MAX_DEPTH = 50;

    private final OrderGateway gateway;

    OrderController(OrderGateway gateway) {
        this.gateway = gateway;
    }

    @PostMapping("/orders")
    @PreAuthorize("hasRole('TRADER')")
    public ResponseEntity<OrderResponse> placeOrder(
            @Valid @RequestBody PlaceOrderRequest request, Authentication authentication) {
        OrderOutcome outcome = gateway.submit(request.toEngineRequest(authentication.getName()));
        MatchResult result = outcome.result();
        if (result.status() == OrderStatus.REJECTED) {
            throw new OrderRejectedException(result.orderId(), result.rejectReason());
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderResponse.from(outcome));
    }

    /** 404 for someone else's order as well as for an unknown one, so callers can't probe other accounts. */
    @DeleteMapping("/orders/{symbol}/{orderId}")
    @PreAuthorize("hasRole('TRADER')")
    public ResponseEntity<Void> cancelOrder(
            @PathVariable String symbol, @PathVariable long orderId, Authentication authentication) {
        return gateway.cancel(symbol, orderId, authentication.getName())
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    @GetMapping("/books/{symbol}")
    public BookResponse book(@PathVariable String symbol, @RequestParam(defaultValue = "10") int depth) {
        return BookResponse.from(gateway.book(symbol, Math.clamp(depth, 1, MAX_DEPTH)));
    }
}
