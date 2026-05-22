package com.recon.agent.tools;

import com.recon.common.model.Resolution;
import com.recon.ledger.repo.ReconCaseRepository;
import com.recon.pii.PiiRedactor;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Terminal tool: the only one that mutates recon_cases.
 * Write guard: UPDATE WHERE resolution IS NULL — stale writes return 0 rows.
 *
 * The LLM-generated rationale is redacted via PiiRedactor before being
 * persisted as notes, so PII the model may have inferred never reaches the DB.
 */
@Component
public class ProposeResolutionTool {

    private static final Logger log = LoggerFactory.getLogger(ProposeResolutionTool.class);

    private final ReconCaseRepository caseRepo;
    private final PiiRedactor         piiRedactor;

    public ProposeResolutionTool(ReconCaseRepository caseRepo, PiiRedactor piiRedactor) {
        this.caseRepo    = caseRepo;
        this.piiRedactor = piiRedactor;
    }

    @Tool("Propose a resolution for a reconciliation case. Terminal call — must be called exactly once per case.")
    public Map<String, Object> proposeResolution(
            @P("unique case identifier") String caseUid,
            @P("resolution type: PROPOSED (confident) or ESCALATE (insufficient evidence)") String resolutionType,
            @P("confidence score between 0.0 and 1.0") double confidence,
            @P("rationale explaining the resolution decision") String rationale,
            @P("matched PG transaction ID; pass 0 if not applicable") long pgTxnId) {

        Resolution resolution  = Resolution.valueOf(resolutionType);
        Long       txnId       = pgTxnId == 0 ? null : pgTxnId;
        // Redact before persistence: the LLM may embed PII (VPA, phone, PAN) in its rationale.
        String     safeRationale = piiRedactor.redact(rationale);
        String     notes       = "{\"rationale\": \"" + safeRationale.replace("\"", "\\\"") + "\"}";

        boolean updated = caseRepo.proposeResolution(caseUid, resolution,
                BigDecimal.valueOf(confidence), "agent", txnId, notes);

        if (!updated) {
            log.warn("Stale write detected for case_uid={} — already resolved", caseUid);
            return Map.of("status", "STALE", "case_uid", caseUid);
        }

        log.info("Resolution proposed: case_uid={} resolution={} confidence={}", caseUid, resolutionType, confidence);
        return Map.of("status", "OK", "case_uid", caseUid, "resolution", resolutionType);
    }
}
