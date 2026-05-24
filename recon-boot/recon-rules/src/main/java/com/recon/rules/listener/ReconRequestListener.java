package com.recon.rules.listener;

import com.recon.common.event.CaseApprovedEvent;
import com.recon.common.event.ReconRequestEvent;
import com.recon.common.model.MatchType;
import com.recon.common.model.RuleMatch;
import com.recon.common.model.SettlementLine;
import com.recon.ledger.repo.ReconCaseRepository;
import com.recon.ledger.repo.SettlementLineRepository;
import com.recon.rules.engine.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

@Component
public class ReconRequestListener {

    private static final Logger log = LoggerFactory.getLogger(ReconRequestListener.class);

    private final List<Rule>               rules;
    private final SettlementLineRepository settlementRepo;
    private final ReconCaseRepository      caseRepo;
    private final ApplicationEventPublisher eventPublisher;

    public ReconRequestListener(List<Rule> rules,
                                SettlementLineRepository settlementRepo,
                                ReconCaseRepository caseRepo,
                                ApplicationEventPublisher eventPublisher) {
        this.rules          = rules;
        this.settlementRepo = settlementRepo;
        this.caseRepo       = caseRepo;
        this.eventPublisher = eventPublisher;
    }

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onReconRequest(ReconRequestEvent event) {
        String caseUid = event.caseUid();
        log.debug("Processing recon request case_uid={}", caseUid);

        try {
            Optional<SettlementLine> lineOpt =
                    settlementRepo.findByFileIdAndLineNo(event.fileId(), event.lineNo());

            if (lineOpt.isEmpty()) {
                log.warn("Settlement line not found file_id={} line_no={} — skipping",
                        event.fileId(), event.lineNo());
                return;
            }

            SettlementLine line = lineOpt.get();
            RuleMatch match = evaluate(line);

            switch (match.matchType()) {
                case EXACT, TOLERANCE, DUPLICATE -> {
                    caseRepo.upsertAutoResolved(caseUid, line.id(), match.pgTransactionId(),
                            match.matchType(), match.confidence());
                    eventPublisher.publishEvent(new CaseApprovedEvent(caseUid));
                    log.info("AUTO_RESOLVED case_uid={} match={} conf={}",
                            caseUid, match.matchType(), match.confidence());
                }
                default -> {
                    // upsertPending atomically writes a 'recon.investigate' outbox row;
                    // agent-worker's InvestigateOutboxDrainer polls that row across the JVM boundary.
                    caseRepo.upsertPending(caseUid, line.id(), match.matchType());
                    log.info("ROUTED_TO_AGENT case_uid={} match={}", caseUid, match.matchType());
                }
            }
        } catch (Exception e) {
            log.error("Error processing case_uid={}", caseUid, e);
            throw e;
        }
    }

    private RuleMatch evaluate(SettlementLine line) {
        for (Rule rule : rules) {
            RuleMatch m = rule.evaluate(line, new HashSet<>());
            if (m.matched()) return m;
        }
        return RuleMatch.of(MatchType.UNKNOWN, 0.0, null);
    }
}
