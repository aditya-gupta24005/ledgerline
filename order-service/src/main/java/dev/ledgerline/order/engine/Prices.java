package dev.ledgerline.order.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Converts between decimal prices at the API boundary and the integer ticks the engine matches on.
 * One tick is 0.0001, so no rounding ever happens silently.
 */
public final class Prices {

    public static final int SCALE = 4;

    private Prices() {
    }

    public static long toTicks(BigDecimal price) {
        try {
            return price.setScale(SCALE, RoundingMode.UNNECESSARY).unscaledValue().longValueExact();
        } catch (ArithmeticException e) {
            throw new InvalidPriceException(
                    "price " + price.toPlainString() + " must have at most " + SCALE + " decimal places");
        }
    }

    public static BigDecimal fromTicks(long ticks) {
        return BigDecimal.valueOf(ticks, SCALE);
    }
}
