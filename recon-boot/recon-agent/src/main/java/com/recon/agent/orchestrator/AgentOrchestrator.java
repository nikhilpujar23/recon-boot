package com.recon.agent.orchestrator;

import com.recon.agent.service.ReconInvestigateAgent;
import com.recon.common.config.AppConfig;
import com.recon.common.model.PgTransaction;
import com.recon.common.model.ReconCase;
import com.recon.common.model.Resolution;
import com.recon.common.model.SettlementLine;
import com.recon.ledger.repo.AgentTraceRepository;
import com.recon.ledger.repo.PgTransactionRepository;
import com.recon.ledger.repo.ReconCaseRepository;
import com.recon.ledger.repo.SettlementLineRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * Runs the LangChain4j-backed investigation agent for a reconciliation case.
 *
 * LangChain4j (AiServices) handles the tool-call loop internally.
 * This class is responsible for:
 *   - wall-clock timeout enforcement
 *   - fail-closed escalation on timeout or exception
 *   - trace persistence after each run
 *
 * PII redaction is enforced at the tool level:
 *   - read tools (search, chargeback, settlement) wrap outputs via RedactionGateway
 *   - ProposeResolutionTool redacts LLM-written rationale via PiiRedactor before DB write
 */
@Service
public class AgentOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(AgentOrchestrator.class);

    private final AppConfig                config;
    private final ReconInvestigateAgent    investigateAgent;
    private final ReconCaseRepository      caseRepo;
    private final SettlementLineRepository settlementRepo;
    private final PgTransactionRepository  txnRepo;
    private final AgentTraceRepository     traceRepo;
    private final ExecutorService          executor = Executors.newCachedThreadPool();
    private final java.util.concurrent.Semaphore llmGate = new java.util.concurrent.Semaphore(1);

    public AgentOrchestrator(AppConfig config, ReconInvestigateAgent investigateAgent,
                             ReconCaseRepository caseRepo, SettlementLineRepository settlementRepo,
                             PgTransactionRepository txnRepo, AgentTraceRepository traceRepo) {
        this.config           = config;
        this.investigateAgent = investigateAgent;
        this.caseRepo         = caseRepo;
        this.settlementRepo   = settlementRepo;
        this.txnRepo          = txnRepo;
        this.traceRepo        = traceRepo;
    }

    public void investigate(String caseUid) {
        long start = System.currentTimeMillis();
        String prompt = buildPrompt(caseUid);
        int delayMs = config.agent().interCallDelayMs();
        Future<?> future = executor.submit(() -> {
            llmGate.acquireUninterruptibly();
            try {
                investigateAgent.investigate(prompt);
            } finally {
                if (delayMs > 0) {
                    try { Thread.sleep(delayMs); } catch (InterruptedException ignored) {}
                }
                llmGate.release();
            }
        });
        try {
            future.get(config.agent().timeoutSeconds(), TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - start;
            traceRepo.insert(caseUid, promptHash(caseUid), config.agent().modelInvest(),
                    List.of(), "[]", elapsed, BigDecimal.ZERO);
        } catch (TimeoutException e) {
            future.cancel(true);
            log.error("Agent timeout case_uid={}", caseUid);
            escalate(caseUid, "Timeout after " + config.agent().timeoutSeconds() + "s");
        } catch (Exception e) {
            log.error("Agent failure case_uid={}", caseUid, e);
            escalate(caseUid, "Exception: " + e.getMessage());
        }
    }

    private String buildPrompt(String caseUid) {
        Optional<ReconCase> caseOpt = caseRepo.findByCaseUid(caseUid);
        if (caseOpt.isEmpty()) {
            return "Investigate reconciliation case: " + caseUid + "\n(Case record not found — escalate.)";
        }
        ReconCase reconCase = caseOpt.get();

        Optional<SettlementLine> lineOpt = reconCase.settlementLineId() != null
                ? settlementRepo.findById(reconCase.settlementLineId())
                : Optional.empty();

        Optional<PgTransaction> txnOpt = reconCase.pgTransactionId() != null
                ? txnRepo.findById(reconCase.pgTransactionId())
                : Optional.empty();

        StringBuilder sb = new StringBuilder();
        sb.append("Investigate reconciliation case: ").append(caseUid).append("\n\n");
        sb.append("Match type : ").append(reconCase.matchType()).append("\n");
        sb.append("NPCI status: ").append(reconCase.npciStatus() != null ? reconCase.npciStatus() : "UNKNOWN").append("\n");

        lineOpt.ifPresent(line -> {
            sb.append("\nSettlement file line:\n");
            sb.append("  RRN            : ").append(line.rrn()).append("\n");
            sb.append("  UTR            : ").append(line.utr()).append("\n");
            sb.append("  Amount (paise) : ").append(line.amountPaise()).append("\n");
            sb.append("  Fee (paise)    : ").append(line.feePaise()).append("\n");
            sb.append("  Net (paise)    : ").append(line.netPaise()).append("\n");
            sb.append("  NPCI status    : ").append(line.status()).append("\n");
            sb.append("  File ID        : ").append(line.fileId()).append("\n");
        });

        txnOpt.ifPresent(txn -> {
            sb.append("\nPG transaction (linked by rules engine):\n");
            sb.append("  ID             : ").append(txn.id()).append("\n");
            sb.append("  Txn ID         : ").append(txn.txnId()).append("\n");
            sb.append("  RRN            : ").append(txn.rrn()).append("\n");
            sb.append("  UTR            : ").append(txn.utr()).append("\n");
            sb.append("  Amount (paise) : ").append(txn.amountPaise()).append("\n");
            sb.append("  Status         : ").append(txn.status()).append("\n");
            sb.append("  Created at     : ").append(txn.createdAt()).append("\n");
        });

        if (lineOpt.isPresent() && txnOpt.isPresent()) {
            long diff = lineOpt.get().amountPaise() - txnOpt.get().amountPaise();
            sb.append("\nAmount difference: ").append(Math.abs(diff)).append(" paise");
            sb.append(" (settlement is ").append(diff > 0 ? diff + "p MORE" : Math.abs(diff) + "p LESS")
              .append(" than PG transaction)\n");
        }

        if (txnOpt.isEmpty()) {
            sb.append("\nNo PG transaction linked by the rules engine. ");
            sb.append("Use search_pg_transactions to locate it or confirm it is missing.\n");
        }

        return sb.toString();
    }

    private void escalate(String caseUid, String reason) {
        String safe = reason
                .replace("\\", "\\\\")
                .replace("\"", "'")
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ");
        String notes = "{\"escalation_reason\":\"" + safe + "\"}";
        caseRepo.proposeResolution(caseUid, Resolution.ESCALATE,
                BigDecimal.ZERO, "agent", null, notes);
    }

    private String promptHash(String caseUid) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(("recon-investigate-v1" + caseUid).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash).substring(0, 16);
        } catch (Exception e) {
            return "unknown";
        }
    }
}
