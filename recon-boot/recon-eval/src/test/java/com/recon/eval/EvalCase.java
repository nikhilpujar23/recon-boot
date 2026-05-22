package com.recon.eval;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * POJO mapping for the YAML golden-case files under eval/cases/.
 *
 * Two modes share the same file format:
 *   mode: rules  → settlement_line + stubs.lookup_pg_txns + expected.{match_type, confidence}
 *   mode: agent  → case + stubs + llm_script + expected.{resolution_type, must_call_tools, max_cost_usd}
 */
public class EvalCase {

    public String id;
    public String description;
    public String mode;  // "rules" or "agent"

    // ── rules mode ────────────────────────────────────────────────────────────
    @JsonProperty("settlement_line")
    public Map<String, Object> settlementLine;

    // ── agent mode ────────────────────────────────────────────────────────────
    // "case" is a Java keyword — mapped via @JsonProperty
    @JsonProperty("case")
    public Map<String, Object> caseData;

    @JsonProperty("llm_script")
    public List<Map<String, Object>> llmScript;

    // ── shared ────────────────────────────────────────────────────────────────
    public Map<String, Object> stubs;
    public Map<String, Object> expected;

    // ── accessors ─────────────────────────────────────────────────────────────

    public String expectedMatchType()      { return (String) expected.get("match_type"); }
    public double expectedConfidence()     { return ((Number) expected.get("confidence")).doubleValue(); }
    public String expectedResolutionType() { return (String) expected.get("resolution_type"); }

    @SuppressWarnings("unchecked")
    public List<String> mustCallTools() {
        Object v = expected.get("must_call_tools");
        return v instanceof List ? (List<String>) v : List.of();
    }

    public double maxCostUsd() {
        Object v = expected.get("max_cost_usd");
        return v != null ? ((Number) v).doubleValue() : 0.01;
    }
}
