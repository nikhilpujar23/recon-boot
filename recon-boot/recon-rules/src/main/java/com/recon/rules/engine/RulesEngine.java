package com.recon.rules.engine;

import com.recon.common.model.*;
import com.recon.common.util.CaseUid;
import com.recon.ledger.repo.ReconCaseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Orchestrates the ordered rule pipeline.
 * Each settlement line is evaluated against rules in declaration order; first match wins.
 * EXACT/TOLERANCE/DUPLICATE → AUTO_RESOLVED.
 * AMOUNT_MISMATCH/MISSING_LEG/UNKNOWN → left pending for the agent.
 */
@Service
public class RulesEngine {

    private static final Logger log = LoggerFactory.getLogger(RulesEngine.class);

    private final List<Rule> rules;
    private final ReconCaseRepository caseRepo;

    public RulesEngine(List<Rule> rules, ReconCaseRepository caseRepo) {
        this.rules = rules;
        this.caseRepo = caseRepo;
    }

    public RulesResult processFile(String fileId, List<SettlementLine> lines) {
        Set<String> seenRrns = new HashSet<>();
        int autoResolved = 0;
        int pendingAgent = 0;

        for (SettlementLine line : lines) {
            String caseUid = CaseUid.of(fileId, line.lineNo());
            RuleMatch match = evaluate(line, seenRrns);

            if (line.rrn() != null && !line.rrn().isBlank()) seenRrns.add(line.rrn());

            switch (match.matchType()) {
                case EXACT, TOLERANCE, DUPLICATE -> {
                    caseRepo.upsertAutoResolved(caseUid, line.id(), match.pgTransactionId(),
                            match.matchType(), match.confidence());
                    autoResolved++;
                    log.debug("AUTO_RESOLVED caseUid={} match={} conf={}", caseUid, match.matchType(), match.confidence());
                }
                default -> {
                    caseRepo.upsertPending(caseUid, line.id(), match.matchType());
                    pendingAgent++;
                    log.debug("PENDING_AGENT caseUid={} match={}", caseUid, match.matchType());
                }
            }
        }

        return new RulesResult(autoResolved, pendingAgent);
    }

    private RuleMatch evaluate(SettlementLine line, Set<String> seenRrns) {
        for (Rule rule : rules) {
            RuleMatch m = rule.evaluate(line, seenRrns);
            if (m.matched()) return m;
        }
        return RuleMatch.of(MatchType.UNKNOWN, 0.0, null);
    }

    public record RulesResult(int autoResolved, int pendingAgent) {}
}
