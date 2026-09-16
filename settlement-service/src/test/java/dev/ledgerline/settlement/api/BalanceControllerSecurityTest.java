package dev.ledgerline.settlement.api;

import static dev.ledgerline.settlement.TestUsers.ops;
import static dev.ledgerline.settlement.TestUsers.risk;
import static dev.ledgerline.settlement.TestUsers.trader;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.ledgerline.security.LedgerlineSecurityAutoConfiguration;
import dev.ledgerline.settlement.persistence.AssetBalance;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(BalanceController.class)
@Import(LedgerlineSecurityAutoConfiguration.class)
class BalanceControllerSecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JournalEntryRepository journal;

    @BeforeEach
    void bobHasABalance() {
        when(journal.balancesFor("bob")).thenReturn(List.of(new AssetBalance("USD", new BigDecimal("10.0000"))));
    }

    @Test
    void aTraderCanReadTheirOwnBalances() throws Exception {
        mockMvc.perform(get("/api/v1/accounts/bob/balances").with(trader("bob")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].asset").value("USD"));
    }

    @Test
    void aTraderCannotReadAnotherAccount() throws Exception {
        mockMvc.perform(get("/api/v1/accounts/bob/balances").with(trader("alice")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.title").value("Forbidden"));
    }

    @Test
    void riskAndOpsCanReadAnyAccount() throws Exception {
        mockMvc.perform(get("/api/v1/accounts/bob/balances").with(risk())).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/accounts/bob/balances").with(ops())).andExpect(status().isOk());
    }

    @Test
    void noTokenIsUnauthorized() throws Exception {
        mockMvc.perform(get("/api/v1/accounts/bob/balances")).andExpect(status().isUnauthorized());
    }
}
