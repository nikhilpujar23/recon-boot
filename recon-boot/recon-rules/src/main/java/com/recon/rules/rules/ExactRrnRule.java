package com.recon.rules.rules;

import com.recon.common.model.MatchType;
import com.recon.common.model.PgTransaction;
import com.recon.common.model.RuleMatch;
import com.recon.common.model.SettlementLine;
import com.recon.ledger.repo.PgTransactionRepository;
import com.recon.rules.engine.Rule;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;

/** Rule 1: RRN + amount + status=SUCCESS → EXACT match, confidence 1.0. */
@Component
@Order(1)
public class ExactRrnRule implements Rule {

    private final PgTransactionRepository txnRepo;

    public ExactRrnRule(PgTransactionRepository txnRepo) {
        this.txnRepo = txnRepo;
    }

    @Override
    public RuleMatch evaluate(SettlementLine line, Set<String> seenRrns) {
        if (line.rrn() == null || line.rrn().isBlank()) return RuleMatch.noMatch();
        Optional<PgTransaction> txnOpt = txnRepo.findByRrn(line.rrn());
        if (txnOpt.isEmpty()) return RuleMatch.noMatch();
        PgTransaction txn = txnOpt.get();
        if (statusesAgree(txn.status(), line.status()) && txn.amountPaise() == line.amountPaise()) {
            return RuleMatch.of(MatchType.EXACT, 1.0, txn.id());
        }
        return RuleMatch.noMatch();
    }
}
