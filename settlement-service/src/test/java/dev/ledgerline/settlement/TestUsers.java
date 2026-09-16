package dev.ledgerline.settlement;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

import java.util.Arrays;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/** Fake bearer tokens for MockMvc: no Keycloak needed, the JWT decoder is never called. */
public final class TestUsers {

    private TestUsers() {
    }

    public static RequestPostProcessor as(String username, String... roles) {
        return jwt()
                .jwt(jwt -> jwt.subject(username).claim("preferred_username", username))
                .authorities(Arrays.stream(roles)
                        .<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                        .toList());
    }

    public static RequestPostProcessor trader(String username) {
        return as(username, "TRADER");
    }

    public static RequestPostProcessor risk() {
        return as("rita", "RISK");
    }

    public static RequestPostProcessor ops() {
        return as("oscar", "OPS");
    }
}
