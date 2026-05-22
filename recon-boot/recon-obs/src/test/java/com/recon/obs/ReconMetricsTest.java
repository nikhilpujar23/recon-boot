package com.recon.obs;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReconMetricsTest {

    private MeterRegistry registry;
    private ReconMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics  = new ReconMetrics(registry);
    }

    @Test
    void manualReviewQueueSizeGaugeReflectsSet() {
        metrics.setManualReviewQueueSize(42);
        double value = registry.get("manual_review_queue_size").gauge().value();
        assertEquals(42.0, value);
    }

    @Test
    void reconDlqSizeGaugeReflectsSet() {
        metrics.setReconDlqSize(7);
        double value = registry.get("recon_dlq_size").gauge().value();
        assertEquals(7.0, value);
    }

    @Test
    void reconMatchRateGaugeReflectsSet() {
        metrics.setReconMatchRate(0.95);
        double value = registry.get("recon_match_rate").gauge().value();
        assertEquals(0.95, value, 1e-9);
    }

    @Test
    void reconCasesTotalCounterIncrements() {
        metrics.incrementReconCases("EXACT", "rules");
        metrics.incrementReconCases("EXACT", "rules");
        metrics.incrementReconCases("AMOUNT_MISMATCH", "agent");

        double exactCount = registry.get("recon_cases_total")
                .tag("match_type", "EXACT")
                .tag("resolved_by", "rules")
                .counter().count();
        assertEquals(2.0, exactCount);

        double agentCount = registry.get("recon_cases_total")
                .tag("match_type", "AMOUNT_MISMATCH")
                .tag("resolved_by", "agent")
                .counter().count();
        assertEquals(1.0, agentCount);
    }

    @Test
    void llmCostCounterAccumulatesCorrectly() {
        metrics.incrementLlmCost("claude-haiku-4-5", 0.0005);
        metrics.incrementLlmCost("claude-haiku-4-5", 0.0003);

        double total = registry.get("llm_cost_usd_total")
                .tag("model", "claude-haiku-4-5")
                .counter().count();
        assertEquals(0.0008, total, 1e-9);
    }

    @Test
    void llmTokensCounterTaggedByKind() {
        metrics.incrementLlmTokens("claude-sonnet-4-6", "input", 1000);
        metrics.incrementLlmTokens("claude-sonnet-4-6", "cached", 400);

        double input  = registry.get("llm_tokens_total").tag("kind", "input").counter().count();
        double cached = registry.get("llm_tokens_total").tag("kind", "cached").counter().count();
        assertEquals(1000, input, 1e-9);
        assertEquals(400, cached, 1e-9);
    }

    @Test
    void staleWriteCounterIncrements() {
        metrics.incrementAgentStaleWrite();
        metrics.incrementAgentStaleWrite();
        assertEquals(2.0, registry.get("agent_stale_write_total").counter().count());
    }

    @Test
    void redactionFailureCounterIncrements() {
        metrics.incrementRedactionFailure();
        assertEquals(1.0, registry.get("redaction_failure_total").counter().count());
    }

    @Test
    void rulesEngineDurationTimerRecords() {
        metrics.recordRulesEngineDuration(12);
        metrics.recordRulesEngineDuration(8);
        assertEquals(2, registry.get("recon_rules_engine_duration_seconds").timer().count());
    }

    @Test
    void agentDurationTimerRecords() {
        metrics.recordAgentDuration(500);
        assertEquals(1, registry.get("recon_agent_duration_seconds").timer().count());
    }
}
