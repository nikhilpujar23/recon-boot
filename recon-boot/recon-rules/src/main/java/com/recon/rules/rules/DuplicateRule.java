package com.recon.rules.rules;

import com.recon.common.model.MatchType;
import com.recon.common.model.RuleMatch;
import com.recon.common.model.SettlementLine;
import com.recon.rules.engine.Rule;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Set;

/** Rule 4: Same RRN appeared earlier in this file → DUPLICATE, confidence 0.90. */
@Component
@Order(4)
public class DuplicateRule implements Rule {

    @Override
    public RuleMatch evaluate(SettlementLine line, Set<String> seenRrns) {
        if (line.rrn() == null || line.rrn().isBlank()) return RuleMatch.noMatch();
        if (seenRrns.contains(line.rrn())) {
            return RuleMatch.of(MatchType.DUPLICATE, 0.90, null);
        }
        return RuleMatch.noMatch();
    }
}
