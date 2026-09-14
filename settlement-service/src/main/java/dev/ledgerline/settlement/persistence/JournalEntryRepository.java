package dev.ledgerline.settlement.persistence;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface JournalEntryRepository extends JpaRepository<JournalEntry, Long> {

    List<JournalEntry> findByTradeId(String tradeId);

    @Query("""
            select new dev.ledgerline.settlement.persistence.AssetBalance(e.asset, sum(e.amount))
            from JournalEntry e
            where e.accountId = :accountId
            group by e.asset
            order by e.asset""")
    List<AssetBalance> balancesFor(String accountId);
}
