package dev.ledgerline.settlement;

/** Wire-format JSON for a valid trade: bob buys 10 ACME from alice at 101.5. */
public final class TradeJson {

    private TradeJson() {
    }

    public static String valid(String tradeId) {
        return """
                {"tradeId":"%s","symbol":"ACME","currency":"USD","price":101.5,"quantity":10,\
                "buyOrderId":1,"buyAccountId":"bob","sellOrderId":2,"sellAccountId":"alice",\
                "aggressorSide":"BUY","executedAt":"2026-09-15T10:00:00Z"}""".formatted(tradeId);
    }
}
