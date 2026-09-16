package dev.ledgerline.security;

import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.security.autoconfigure.actuate.web.servlet.ManagementWebSecurityAutoConfiguration;
import org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration;
import org.springframework.boot.security.oauth2.server.resource.autoconfigure.web.OAuth2ResourceServerWebSecurityAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

/**
 * Stateless JWT resource server for every Ledgerline service.
 *
 * <p>Issuer and audience checks come from the standard Boot properties
 * ({@code spring.security.oauth2.resourceserver.jwt.issuer-uri} and {@code …jwt.audiences}). This class adds
 * the Keycloak role mapping, the username principal, CORS for the dashboard, and method security so each
 * controller can state its own rule with {@code @PreAuthorize}.
 */
@AutoConfiguration(before = {
        ServletWebSecurityAutoConfiguration.class,
        OAuth2ResourceServerWebSecurityAutoConfiguration.class,
        ManagementWebSecurityAutoConfiguration.class})
@EnableWebSecurity
@EnableMethodSecurity
@EnableConfigurationProperties(LedgerlineSecurityProperties.class)
public class LedgerlineSecurityAutoConfiguration {

    @Bean
    SecurityFilterChain ledgerlineSecurityFilterChain(HttpSecurity http, LedgerlineSecurityProperties properties)
            throws Exception {
        JwtAuthenticationConverter jwtConverter = new JwtAuthenticationConverter();
        jwtConverter.setPrincipalClaimName(RealmRoleConverter.USERNAME_CLAIM);
        jwtConverter.setJwtGrantedAuthoritiesConverter(new RealmRoleConverter());

        http
                .csrf(AbstractHttpConfigurer::disable)
                .cors(cors -> cors.configurationSource(corsConfigurationSource(properties)))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(requests -> requests
                        .requestMatchers("/actuator/health", "/actuator/health/**", "/actuator/info").permitAll()
                        .anyRequest().authenticated())
                .oauth2ResourceServer(resourceServer -> resourceServer
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtConverter)));
        return http.build();
    }

    @Bean
    SecurityProblemHandler securityProblemHandler() {
        return new SecurityProblemHandler();
    }

    private static CorsConfigurationSource corsConfigurationSource(LedgerlineSecurityProperties properties) {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(properties.cors().allowedOrigins());
        configuration.setAllowedMethods(List.of("GET", "POST", "DELETE", "OPTIONS"));
        configuration.setAllowedHeaders(List.of("Authorization", "Content-Type"));
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }
}
