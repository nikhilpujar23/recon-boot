package com.recon.rules.rules;

import com.recon.common.model.MatchType;
import com.recon.common.model.PgTransaction;
import com.recon.common.model.RuleMatch;
import com.recon.common.model.SettlementLine;
import com.recon.ledger.repo.PgTransactionRepository;
import com.recon.rules.engine.Rule;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;

/** Rule 2: UTR + amount same day → EXACT match, confidence 0.99. */
@Component
@Order(2)
public class UtrAmountRule implements Rule {

    private final PgTransactionRepository txnRepo;

    public UtrAmountRule(PgTransactionRepository txnRepo) {
        this.txnRepo = txnRepo;
    }

    @Override
    public RuleMatch evaluate(SettlementLine line, Set<String> seenRrns) {
        if (line.utr() == null || line.utr().isBlank()) return RuleMatch.noMatch();
        Optional<PgTransaction> txnOpt = txnRepo.findByUtr(line.utr());
        if (txnOpt.isEmpty()) return RuleMatch.noMatch();
        PgTransaction txn = txnOpt.get();
        if (statusesAgree(txn.status(), line.status()) && txn.amountPaise() == line.amountPaise()) {
            // Require same calendar day (UTC) to avoid matching older transactions with same UTR
            LocalDate settlementDate = line.ingestedAt().atZone(ZoneOffset.UTC).toLocalDate();
            LocalDate txnDate = txn.createdAt().atZone(ZoneOffset.UTC).toLocalDate();
            if (settlementDate.equals(txnDate)) {
                return RuleMatch.of(MatchType.EXACT, 0.99, txn.id());
            } else {
                return RuleMatch.noMatch();
            }
        }
        return RuleMatch.noMatch();
    }
}
