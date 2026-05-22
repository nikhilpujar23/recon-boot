package com.recon.common.model;

import java.math.BigDecimal;

/**
 * Immutable result returned by each Rule implementation.
 * matched=false means this rule did not fire; the engine tries the next rule.
 */
public record RuleMatch(
        boolean matched,
        MatchType matchType,
        BigDecimal confidence,
        Long pgTransactionId
) {
    public static RuleMatch noMatch() {
        return new RuleMatch(false, null, null, null);
    }

    public static RuleMatch of(MatchType type, double confidence, Long pgTxnId) {
        return new RuleMatch(true, type, BigDecimal.valueOf(confidence), pgTxnId);
    }
}
