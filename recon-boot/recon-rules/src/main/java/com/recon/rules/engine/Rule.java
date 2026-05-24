package com.recon.rules.engine;

import com.recon.common.model.RuleMatch;
import com.recon.common.model.SettlementLine;
import com.recon.common.model.TxnStatus;

import java.util.Set;

/** Stateless rule: given a settlement line and the set of RRNs seen so far, return a match or NO_MATCH. */
public interface Rule {
    RuleMatch evaluate(SettlementLine line, Set<String> seenRrns);

    /** True if both sides agree on SUCCESS or FAILED. Any other/unrecognised status → false (route to agent). */
    default boolean statusesAgree(TxnStatus pgStatus, String settlementStatus) {
        if (settlementStatus == null) return false;
        TxnStatus normalized = switch (settlementStatus.trim().toUpperCase()) {
            case "SUCCESS" -> TxnStatus.SUCCESS;
            case "FAILED", "FAILURE" -> TxnStatus.FAILED;
            default -> null;
        };
        return normalized != null && normalized == pgStatus;
    }
}
