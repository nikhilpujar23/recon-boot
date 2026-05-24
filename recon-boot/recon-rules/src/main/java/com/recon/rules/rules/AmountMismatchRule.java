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

/** Rule 5: RRN matches but amount differs by more than tolerance → AMOUNT_MISMATCH, routes to agent. */
@Component
@Order(5)
public class AmountMismatchRule implements Rule {

    private final PgTransactionRepository txnRepo;

    public AmountMismatchRule(PgTransactionRepository txnRepo) {
        this.txnRepo = txnRepo;
    }

    @Override
    public RuleMatch evaluate(SettlementLine line, Set<String> seenRrns) {
        if (line.rrn() == null || line.rrn().isBlank()) return RuleMatch.noMatch();
        Optional<PgTransaction> txnOpt = txnRepo.findByRrn(line.rrn());
        if (txnOpt.isEmpty()) return RuleMatch.noMatch();
        PgTransaction txn = txnOpt.get();
        if (txn.amountPaise() != line.amountPaise()) {
            return RuleMatch.of(MatchType.AMOUNT_MISMATCH, 0.5, txn.id());
        }
        return RuleMatch.noMatch();
    }
}
