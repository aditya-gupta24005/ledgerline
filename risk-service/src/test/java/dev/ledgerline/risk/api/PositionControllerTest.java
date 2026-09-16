package dev.ledgerline.risk.api;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.risk.query.PositionQueryService;
import dev.ledgerline.risk.query.PositionView;
import dev.ledgerline.risk.query.RiskDataUnavailableException;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PositionController.class)
class PositionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private PositionQueryService queryService;

    @Test
    void returnsTheAccountsPositions() throws Exception {
        when(queryService.positionsFor("alice")).thenReturn(List.of(new PositionView("ACME", 10,
                new BigDecimal("100.0000"), new BigDecimal("110.0000"), new BigDecimal("1100.0000"),
                new BigDecimal("0.0000"), new BigDecimal("100.0000"))));

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].symbol").value("ACME"))
                .andExpect(jsonPath("$[0].netQuantity").value(10))
                .andExpect(jsonPath("$[0].averageCost").value(100.0))
                .andExpect(jsonPath("$[0].lastPrice").value(110.0))
                .andExpect(jsonPath("$[0].notional").value(1100.0))
                .andExpect(jsonPath("$[0].unrealizedPnl").value(100.0));
    }

    @Test
    void returns503WhileRiskDataIsNotQueryable() throws Exception {
        when(queryService.positionsFor("alice")).thenThrow(new RiskDataUnavailableException("stream state: REBALANCING"));

        mockMvc.perform(get("/api/v1/risk/accounts/alice/positions"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("Risk data unavailable"));
    }
}
