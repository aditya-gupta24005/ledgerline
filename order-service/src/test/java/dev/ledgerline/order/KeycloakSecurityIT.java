package dev.ledgerline.order;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.order.outbox.OutboxWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/**
 * Real Keycloak, real tokens, real JWT validation: issuer, signature, audience and role mapping all
 * exercised end to end. The same realm file Compose imports is mounted into the container.
 */
@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ledgerline.scheduling.enabled=false"})
@AutoConfigureMockMvc
@Import(PostgresTestConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class KeycloakSecurityIT {

    @Container
    static GenericContainer<?> keycloak = new GenericContainer<>("quay.io/keycloak/keycloak:26.7.3")
            .withCommand("start-dev", "--import-realm")
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("..", "keycloak", "ledgerline-realm.json")),
                    "/opt/keycloak/data/import/ledgerline-realm.json")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/realms/ledgerline").forPort(8080).forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    @DynamicPropertySource
    static void issuer(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", KeycloakSecurityIT::realmUrl);
    }

    static String realmUrl() {
        return "http://localhost:" + keycloak.getMappedPort(8080) + "/realms/ledgerline";
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OutboxWriter outboxWriter;

    private static String bearer(String username) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "password");
        form.add("client_id", "ledgerline-cli");
        form.add("username", username);
        form.add("password", username);
        Map<?, ?> response = RestClient.create()
                .post()
                .uri(realmUrl() + "/protocol/openid-connect/token")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(form)
                .retrieve()
                .body(Map.class);
        return "Bearer " + response.get("access_token");
    }

    @Test
    void aTraderTokenFromKeycloakPlacesAnOrderUnderTheirUsername() throws Exception {
        mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, bearer("alice"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"symbol": "KCLK", "side": "SELL", "type": "LIMIT", "price": 12.5, "quantity": 3}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("NEW"));

        mockMvc.perform(get("/api/v1/books/KCLK").header(HttpHeaders.AUTHORIZATION, bearer("rita")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.asks[0].quantity").value(3));
    }

    @Test
    void anOpsTokenCannotTrade() throws Exception {
        mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, bearer("oscar"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"symbol": "KCLK", "side": "BUY", "type": "LIMIT", "price": 12.5, "quantity": 1}
                                """))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.title").value("Forbidden"));
    }

    @Test
    void aTamperedTokenIsRejected() throws Exception {
        String token = bearer("alice");
        String tampered = token.substring(0, token.length() - 4) + "AAAA";

        mockMvc.perform(get("/api/v1/books/KCLK").header(HttpHeaders.AUTHORIZATION, tampered))
                .andExpect(status().isUnauthorized());
    }
}
