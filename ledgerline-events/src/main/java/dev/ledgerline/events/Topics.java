package dev.ledgerline.events;

public final class Topics {

    public static final String TRADES_EXECUTED = "ledgerline.trades.executed";
    public static final String TRADES_EXECUTED_DLT = TRADES_EXECUTED + ".DLT";
    public static final String RISK_ALERTS = "ledgerline.risk.alerts";

    private Topics() {
    }
}
