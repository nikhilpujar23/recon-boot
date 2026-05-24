package com.recon.agent.orchestrator;

import com.recon.agent.service.ReconInvestigateAgent;
import com.recon.common.config.AppConfig;
import com.recon.common.model.Resolution;
import com.recon.ledger.repo.AgentTraceRepository;
import com.recon.ledger.repo.ReconCaseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
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

    private final AppConfig             config;
    private final ReconInvestigateAgent investigateAgent;
    private final ReconCaseRepository   caseRepo;
    private final AgentTraceRepository  traceRepo;
    private final ExecutorService       executor = Executors.newCachedThreadPool();

    public AgentOrchestrator(AppConfig config, ReconInvestigateAgent investigateAgent,
                             ReconCaseRepository caseRepo, AgentTraceRepository traceRepo) {
        this.config           = config;
        this.investigateAgent = investigateAgent;
        this.caseRepo         = caseRepo;
        this.traceRepo        = traceRepo;
    }

    public void investigate(String caseUid) {
        long start = System.currentTimeMillis();
        Future<?> future = executor.submit(() ->
                investigateAgent.investigate("Investigate reconciliation case: " + caseUid));
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
