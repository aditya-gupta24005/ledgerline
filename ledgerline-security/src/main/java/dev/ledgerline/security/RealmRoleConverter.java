package dev.ledgerline.security;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

/** Maps Keycloak's {@code realm_access.roles} to {@code ROLE_*} authorities, keeping only Ledgerline's roles. */
public final class RealmRoleConverter implements Converter<Jwt, Collection<GrantedAuthority>> {

    public static final String USERNAME_CLAIM = "preferred_username";

    private static final Set<String> KNOWN_ROLES = Set.of("TRADER", "RISK", "OPS");

    @Override
    public Collection<GrantedAuthority> convert(Jwt jwt) {
        Map<String, Object> realmAccess = jwt.getClaimAsMap("realm_access");
        if (realmAccess == null || !(realmAccess.get("roles") instanceof Collection<?> roles)) {
            return List.of();
        }
        return roles.stream()
                .map(String::valueOf)
                .filter(KNOWN_ROLES::contains)
                .<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                .toList();
    }
}
