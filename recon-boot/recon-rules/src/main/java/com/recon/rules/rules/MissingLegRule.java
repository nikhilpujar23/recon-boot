package com.recon.rules.rules;

import com.recon.common.model.MatchType;
import com.recon.common.model.RuleMatch;
import com.recon.common.model.SettlementLine;
import com.recon.ledger.repo.PgTransactionRepository;
import com.recon.rules.engine.Rule;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Set;

/** Rule 6: No match for RRN or UTR → MISSING_LEG, routes to agent. */
@Component
@Order(6)
public class MissingLegRule implements Rule {

    private final PgTransactionRepository txnRepo;

    public MissingLegRule(PgTransactionRepository txnRepo) {
        this.txnRepo = txnRepo;
    }

    @Override
    public RuleMatch evaluate(SettlementLine line, Set<String> seenRrns) {
        boolean noRrn = line.rrn() == null || line.rrn().isBlank()
                || txnRepo.findByRrn(line.rrn()).isEmpty();
        boolean noUtr = line.utr() == null || line.utr().isBlank()
                || txnRepo.findByUtr(line.utr()).isEmpty();
        if (noRrn && noUtr) {
            return RuleMatch.of(MatchType.MISSING_LEG, 0.3, null);
        }
        return RuleMatch.noMatch();
    }
}
