package dev.ledgerline.security;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("ledgerline.security")
public record LedgerlineSecurityProperties(@DefaultValue Cors cors) {

    public record Cors(@DefaultValue("http://localhost:5173") List<String> allowedOrigins) {
    }
}
