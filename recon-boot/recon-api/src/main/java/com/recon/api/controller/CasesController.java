package com.recon.api.controller;

import com.recon.api.dto.CaseResponse;
import com.recon.api.dto.ErrorResponse;
import com.recon.common.event.CaseApprovedEvent;
import com.recon.common.model.AgentTrace;
import com.recon.common.model.MatchType;
import com.recon.common.model.ReconCase;
import com.recon.common.model.Resolution;
import com.recon.ledger.repo.AgentTraceRepository;
import com.recon.ledger.repo.ReconCaseRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1")
public class CasesController {

    private final ReconCaseRepository    caseRepo;
    private final AgentTraceRepository   traceRepo;
    private final ApplicationEventPublisher eventPublisher;

    public CasesController(ReconCaseRepository caseRepo, AgentTraceRepository traceRepo,
                            ApplicationEventPublisher eventPublisher) {
        this.caseRepo       = caseRepo;
        this.traceRepo      = traceRepo;
        this.eventPublisher = eventPublisher;
    }

    @GetMapping("/cases")
    public ResponseEntity<?> listCases(
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String matchType,
            @RequestParam(defaultValue = "0") long cursor,
            @RequestParam(defaultValue = "50") int limit,
            @RequestAttribute("requestId") String requestId) {

        Resolution res = resolution != null ? Resolution.valueOf(resolution.toUpperCase()) : null;
        MatchType  mt  = matchType  != null ? MatchType.valueOf(matchType.toUpperCase())  : null;

        List<CaseResponse> cases = caseRepo.findPage(res, mt, cursor, limit)
                .stream().map(CaseResponse::from).toList();

        Long nextCursor = cases.isEmpty() ? null
                : caseRepo.findPage(res, mt, cursor, limit).stream()
                .mapToLong(c -> c.id()).max().orElse(0L);

        return ResponseEntity.ok(Map.of("cases", cases, "next_cursor", nextCursor != null ? nextCursor : 0));
    }

    @GetMapping("/cases/{caseUid}")
    public ResponseEntity<?> getCase(
            @PathVariable String caseUid,
            @RequestAttribute("requestId") String requestId) {

        Optional<ReconCase> caseOpt = caseRepo.findByCaseUid(caseUid);
        if (caseOpt.isEmpty()) {
            return ResponseEntity.status(404).body(
                    ErrorResponse.of("NOT_FOUND", "Case not found: " + caseUid, requestId));
        }

        Optional<AgentTrace> trace = traceRepo.findLatestByCaseUid(caseUid);
        return ResponseEntity.ok(Map.of(
                "case", CaseResponse.from(caseOpt.get()),
                "last_trace", trace.map(t -> Map.of(
                        "model", t.model(),
                        "tools_used", t.toolsUsed(),
                        "total_ms", t.totalMs(),
                        "cost_usd", t.costUsd(),
                        "created_at", t.createdAt().toString()
                )).orElse(Map.of())
        ));
    }

    @PostMapping("/cases/{caseUid}/approve")
    public ResponseEntity<?> approveCase(
            @PathVariable String caseUid,
            @RequestBody Map<String, String> body,
            @RequestAttribute("requestId") String requestId) {

        String reviewer = body.get("reviewer_email");
        String comment  = body.get("comment");

        boolean updated = caseRepo.approve(caseUid, reviewer, comment);
        if (!updated) {
            return ResponseEntity.status(409).body(
                    ErrorResponse.of("STALE_RESOLUTION",
                            "Case is not in PROPOSED state or does not exist", requestId));
        }
        ReconCase reconCase = caseRepo.findByCaseUid(caseUid).orElse(null);
        Long pgTxnId    = reconCase != null ? reconCase.pgTransactionId() : null;
        String npciStatus = reconCase != null ? reconCase.npciStatus()    : null;
        eventPublisher.publishEvent(new CaseApprovedEvent(caseUid, pgTxnId, npciStatus));
        return ResponseEntity.ok(Map.of("status", "APPROVED", "case_uid", caseUid));
    }

    @PostMapping("/cases/{caseUid}/reject")
    public ResponseEntity<?> rejectCase(
            @PathVariable String caseUid,
            @RequestBody Map<String, String> body,
            @RequestAttribute("requestId") String requestId) {

        String reviewer = body.get("reviewer_email");
        String comment  = body.get("comment");

        boolean updated = caseRepo.reject(caseUid, reviewer, comment);
        if (!updated) {
            return ResponseEntity.status(409).body(
                    ErrorResponse.of("STALE_RESOLUTION",
                            "Case is not in PROPOSED state or does not exist", requestId));
        }
        return ResponseEntity.ok(Map.of("status", "REJECTED", "case_uid", caseUid));
    }
}
