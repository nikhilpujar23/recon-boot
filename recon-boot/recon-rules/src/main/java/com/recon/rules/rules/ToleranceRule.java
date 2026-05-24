package com.recon.rules.rules;

import com.recon.common.config.AppConfig;
import com.recon.common.model.MatchType;
import com.recon.common.model.PgTransaction;
import com.recon.common.model.RuleMatch;
import com.recon.common.model.SettlementLine;
import com.recon.ledger.repo.PgTransactionRepository;
import com.recon.rules.engine.Rule;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/** Rule 3: RRN matches + amount within ±tolerancePaise → TOLERANCE, confidence 0.95. */
@Component
@Order(3)
public class ToleranceRule implements Rule {

    private final PgTransactionRepository txnRepo;
    private final long tolerancePaise;

    public ToleranceRule(PgTransactionRepository txnRepo, AppConfig config) {
        this.txnRepo = txnRepo;
        this.tolerancePaise = config.rules().tolerancePaise();
    }

    @Override
    public RuleMatch evaluate(SettlementLine line, Set<String> seenRrns) {
        if (line.rrn() == null || line.rrn().isBlank()) return RuleMatch.noMatch();
        long min = line.amountPaise() - tolerancePaise;
        long max = line.amountPaise() + tolerancePaise;
        List<PgTransaction> candidates = txnRepo.findByRrnAndAmountRange(line.rrn(), min, max);
        if (candidates.isEmpty()) return RuleMatch.noMatch();
        PgTransaction best = candidates.get(0);
        if (statusesAgree(best.status(), line.status())
                && Math.abs(best.amountPaise() - line.amountPaise()) <= tolerancePaise) {
            return RuleMatch.of(MatchType.TOLERANCE, 0.95, best.id());
        }
        return RuleMatch.noMatch();
    }
}
