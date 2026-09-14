package dev.ledgerline.settlement.persistence;

import java.math.BigDecimal;

public record AssetBalance(String asset, BigDecimal balance) {
}
