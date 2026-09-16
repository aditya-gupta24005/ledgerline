package dev.ledgerline.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

class RealmRoleConverterTest {

    private final RealmRoleConverter converter = new RealmRoleConverter();

    private static Jwt jwtWithClaim(String name, Object value) {
        return Jwt.withTokenValue("token").header("alg", "none").subject("alice").claim(name, value).build();
    }

    @Test
    void mapsKnownRealmRolesToRoleAuthorities() {
        Jwt jwt = jwtWithClaim("realm_access", Map.of("roles", List.of("TRADER", "OPS", "offline_access")));

        assertThat(converter.convert(jwt))
                .extracting(GrantedAuthority::getAuthority)
                .containsExactlyInAnyOrder("ROLE_TRADER", "ROLE_OPS");
    }

    @Test
    void tokenWithoutRealmAccessHasNoAuthorities() {
        assertThat(converter.convert(jwtWithClaim("scope", "openid"))).isEmpty();
    }

    @Test
    void malformedRolesClaimIsIgnored() {
        assertThat(converter.convert(jwtWithClaim("realm_access", Map.of("roles", "TRADER")))).isEmpty();
    }
}
