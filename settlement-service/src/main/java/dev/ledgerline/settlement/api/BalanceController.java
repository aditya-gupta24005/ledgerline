package dev.ledgerline.settlement.api;

import dev.ledgerline.settlement.persistence.AssetBalance;
import dev.ledgerline.settlement.persistence.JournalEntryRepository;
import java.util.List;
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

    @GetMapping("/{accountId}/balances")
    List<AssetBalance> balances(@PathVariable String accountId) {
        return journal.balancesFor(accountId);
    }
}
