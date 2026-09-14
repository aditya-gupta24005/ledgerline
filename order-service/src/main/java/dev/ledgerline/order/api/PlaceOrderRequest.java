package dev.ledgerline.order.api;

import dev.ledgerline.matching.OrderRequest;
import dev.ledgerline.matching.OrderType;
import dev.ledgerline.matching.Side;
import dev.ledgerline.order.engine.Prices;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

public record PlaceOrderRequest(
        @NotBlank String accountId,
        @NotBlank @Pattern(regexp = "[A-Z]{1,10}", message = "must be 1-10 uppercase letters") String symbol,
        @NotNull Side side,
        @NotNull OrderType type,
        @Positive BigDecimal price,
        @Positive long quantity) {

    @AssertTrue(message = "price is required for LIMIT orders and must be omitted for MARKET orders")
    public boolean isPriceConsistentWithType() {
        return type == null || (type == OrderType.LIMIT) == (price != null);
    }

    OrderRequest toEngineRequest() {
        return type == OrderType.MARKET
                ? OrderRequest.market(accountId, symbol, side, quantity)
                : OrderRequest.limit(accountId, symbol, side, Prices.toTicks(price), quantity);
    }
}
