package com.recon.rules.engine;

import com.recon.common.model.RuleMatch;
import com.recon.common.model.SettlementLine;
import com.recon.common.model.TxnStatus;

import java.util.Set;

/** Stateless rule: given a settlement line and the set of RRNs seen so far, return a match or NO_MATCH. */
public interface Rule {
    RuleMatch evaluate(SettlementLine line, Set<String> seenRrns);

}
