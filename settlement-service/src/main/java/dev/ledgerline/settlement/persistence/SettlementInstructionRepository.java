package dev.ledgerline.settlement.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SettlementInstructionRepository extends JpaRepository<SettlementInstruction, String> {
}
