package dev.ledgerline.settlement.api;

import dev.ledgerline.settlement.persistence.AssetBalance;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/accounts")
class BalanceController {

    private final JournalEntryRepository journal;

    BalanceController(JournalEntryRepository journal) {
        this.journal = journal;
    }

    /** Risk and ops see every account; a trader only their own (the token's username). */
    @GetMapping("/{accountId}/balances")
    @PreAuthorize("hasAnyRole('RISK', 'OPS') or #accountId == authentication.name")
    public List<AssetBalance> balances(@PathVariable String accountId) {
        return journal.balancesFor(accountId);
    }
}
