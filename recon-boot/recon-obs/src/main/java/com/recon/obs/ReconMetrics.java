package com.recon.obs;

import io.micrometer.core.instrument.*;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Central registry for all Prometheus metrics defined in LLD §13.1.
 *
 * Usage: inject ReconMetrics and call the typed increment/record methods.
 * All metric names match the LLD specification exactly.
 */
@Component
public class ReconMetrics {

    private final MeterRegistry registry;

    // Gauges — backed by AtomicLong so callers can set() without re-registering
    private final AtomicLong manualReviewQueueSize = new AtomicLong(0);
    private final AtomicLong reconDlqSize          = new AtomicLong(0);

    // Match rate is a ratio we track as a gauge via a supplier
    private volatile double reconMatchRate = 0.0;

    public ReconMetrics(MeterRegistry registry) {
        this.registry = registry;

        Gauge.builder("manual_review_queue_size", manualReviewQueueSize, AtomicLong::get)
             .description("Number of recon cases awaiting human review")
             .register(registry);

        Gauge.builder("recon_dlq_size", reconDlqSize, AtomicLong::get)
             .description("Messages sitting in recon.dlq")
             .register(registry);

        Gauge.builder("recon_match_rate", this, m -> m.reconMatchRate)
             .description("Fraction of cases resolved by rules engine (SLO ≥ 0.90)")
             .register(registry);
    }

    // ── Gauge setters ──────────────────────────────────────────────────────────

    public void setManualReviewQueueSize(long size) {
        manualReviewQueueSize.set(size);
    }

    public void setReconDlqSize(long size) {
        reconDlqSize.set(size);
    }

    public void setReconMatchRate(double rate) {
        this.reconMatchRate = rate;
    }

    // ── Counters ───────────────────────────────────────────────────────────────

    /** Increment recon_cases_total with match_type + resolved_by tags. */
    public void incrementReconCases(String matchType, String resolvedBy) {
        Counter.builder("recon_cases_total")
               .description("Total recon cases processed")
               .tag("match_type", matchType)
               .tag("resolved_by", resolvedBy)
               .register(registry)
               .increment();
    }

    /** Increment llm_cost_usd_total for a specific model. */
    public void incrementLlmCost(String model, double costUsd) {
        Counter.builder("llm_cost_usd_total")
               .description("Cumulative LLM spend in USD")
               .tag("model", model)
               .register(registry)
               .increment(costUsd);
    }

    /** Increment llm_tokens_total; kind = "input" | "output" | "cached". */
    public void incrementLlmTokens(String model, String kind, long tokens) {
        Counter.builder("llm_tokens_total")
               .description("Cumulative LLM tokens")
               .tag("model", model)
               .tag("kind", kind)
               .register(registry)
               .increment(tokens);
    }

    /** Increment agent_stale_write_total (concurrent resolution race). */
    public void incrementAgentStaleWrite() {
        Counter.builder("agent_stale_write_total")
               .description("Agent UPDATE returned 0 rows — resolution already written by another worker")
               .register(registry)
               .increment();
    }

    /** Increment redaction_failure_total (fail-closed PII gateway triggered). */
    public void incrementRedactionFailure() {
        Counter.builder("redaction_failure_total")
               .description("PII redaction gateway failures — case escalated")
               .register(registry)
               .increment();
    }

    // ── Histograms ─────────────────────────────────────────────────────────────

    /**
     * Record rules-engine wall-clock duration.
     *
     * @param durationMs milliseconds taken by RulesEngine.run() for one case
     */
    public void recordRulesEngineDuration(long durationMs) {
        Timer.builder("recon_rules_engine_duration_seconds")
             .description("Rules engine latency per case")
             .publishPercentiles(0.5, 0.95, 0.99)
             .register(registry)
             .record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Record agent investigation wall-clock duration.
     *
     * @param durationMs milliseconds taken by AgentOrchestrator.investigate()
     */
    public void recordAgentDuration(long durationMs) {
        Timer.builder("recon_agent_duration_seconds")
             .description("Agent investigation latency per case")
             .publishPercentiles(0.5, 0.95, 0.99)
             .register(registry)
             .record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

}
