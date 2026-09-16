package dev.ledgerline.risk.api;

import static dev.ledgerline.risk.TestUsers.ops;
import static dev.ledgerline.risk.TestUsers.risk;
import static dev.ledgerline.risk.TestUsers.trader;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.risk.query.PositionQueryService;
import dev.ledgerline.risk.query.PositionView;
import dev.ledgerline.risk.query.RiskDataUnavailableException;
import dev.ledgerline.security.LedgerlineSecurityAutoConfiguration;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PositionController.class)
@Import(LedgerlineSecurityAutoConfiguration.class)
class PositionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private PositionQueryService queryService;

    private void aliceHasAPosition() {
        when(queryService.positionsFor("alice")).thenReturn(List.of(new PositionView("ACME", 10,
                new BigDecimal("100.0000"), new BigDecimal("110.0000"), new BigDecimal("1100.0000"),
                new BigDecimal("0.0000"), new BigDecimal("100.0000"))));
    }

    @Test
    void returnsTheCallersOwnPositions() throws Exception {
        aliceHasAPosition();

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(trader("alice")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].symbol").value("ACME"))
                .andExpect(jsonPath("$[0].netQuantity").value(10))
                .andExpect(jsonPath("$[0].averageCost").value(100.0))
                .andExpect(jsonPath("$[0].lastPrice").value(110.0))
                .andExpect(jsonPath("$[0].notional").value(1100.0))
                .andExpect(jsonPath("$[0].unrealizedPnl").value(100.0));
    }

    @Test
    void riskAndOpsCanReadAnyAccount() throws Exception {
        aliceHasAPosition();

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(risk())).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(ops())).andExpect(status().isOk());
    }

    @Test
    void aTraderCannotReadAnotherAccount() throws Exception {
        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(trader("bob")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.title").value("Forbidden"));
    }

    @Test
    void noTokenIsUnauthorized() throws Exception {
        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions")).andExpect(status().isUnauthorized());
    }

    @Test
    void returns503WhileRiskDataIsNotQueryable() throws Exception {
        when(queryService.positionsFor("alice")).thenThrow(new RiskDataUnavailableException("stream state: REBALANCING"));

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions").with(trader("alice")))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("Risk data unavailable"));
    }
}
