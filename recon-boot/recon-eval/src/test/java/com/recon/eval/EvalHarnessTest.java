package com.recon.eval;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.recon.agent.client.GroqRestClient.ContentBlock;
import com.recon.agent.client.GroqRestClient.MessagesRequest;
import com.recon.agent.client.GroqRestClient.MessagesResponse;
import com.recon.common.config.AppConfig;
import com.recon.common.model.*;
import com.recon.rules.engine.Rule;
import com.recon.rules.rules.*;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Golden-case eval harness (LLD §6.12).
 *
 * Loads every YAML file from eval/cases/, runs rules or agent simulation,
 * then applies a regression gate against eval/baseline.json:
 *   - accuracy drop  > 2%  → fail
 *   - avg cost spike > 20% → fail
 *
 * To update the baseline after intentional improvements, delete baseline.json
 * and re-run; the harness will write a fresh one.
 */
class EvalHarnessTest {

    private static final Path CASES_DIR = Path.of("../eval/cases");
    private static final Path BASELINE  = Path.of("../eval/baseline.json");

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static AppConfig appConfig;

    @BeforeAll
    static void setup() {
        appConfig = new AppConfig(
                new AppConfig.Agent("claude-haiku-4-5", "claude-sonnet-4-6", 6, 30, true, 0),
                new AppConfig.Rules(1L),
                new AppConfig.Pii("", ""),
                new AppConfig.Api("changeme", 60),
                new AppConfig.Retry(1.0, 3, 5.0, 25.0)
        );
    }

    // ── main eval test ────────────────────────────────────────────────────────

    @Test
    void allGoldenCasesPass() throws Exception {
        List<EvalCase> cases = loadCases();
        assertFalse(cases.isEmpty(), "No eval cases found under " + CASES_DIR);

        int rulesTotal = 0, rulesPassed = 0;
        int agentTotal = 0, agentPassed = 0;
        double totalCost = 0.0;
        List<String> failures = new ArrayList<>();

        for (EvalCase ec : cases) {
            try {
                if ("rules".equals(ec.mode)) {
                    rulesTotal++;
                    runRulesCase(ec);
                    rulesPassed++;
                } else if ("agent".equals(ec.mode)) {
                    agentTotal++;
                    totalCost += runAgentCase(ec);
                    agentPassed++;
                }
            } catch (AssertionError | Exception e) {
                failures.add("[" + ec.id + "] " + e.getMessage());
            }
        }

        int    total         = rulesTotal + agentTotal;
        int    passed        = rulesPassed + agentPassed;
        double accuracy      = total      > 0 ? (double) passed      / total      : 0.0;
        double rulesAccuracy = rulesTotal > 0 ? (double) rulesPassed / rulesTotal : 0.0;
        double agentAccuracy = agentTotal > 0 ? (double) agentPassed / agentTotal : 0.0;
        double avgCost       = agentTotal > 0 ? totalCost / agentTotal            : 0.0;

        checkRegression(accuracy, avgCost);
        writeBaseline(accuracy, rulesAccuracy, agentAccuracy, avgCost);

        if (!failures.isEmpty()) {
            fail("Eval failures:\n" + String.join("\n", failures));
        }
    }

    // ── rules mode ────────────────────────────────────────────────────────────

    private void runRulesCase(EvalCase ec) {
        SettlementLine       line   = parseSettlementLine(ec.settlementLine);
        List<PgTransaction>  pgTxns = parsePgTxns(stubList(ec.stubs, "lookup_pg_txns"));

        EvalPgTransactionRepository repo  = new EvalPgTransactionRepository(pgTxns);
        List<Rule>                  rules = buildRules(repo);
        RuleMatch                   match = evaluateRules(line, rules);

        String expectedType = ec.expectedMatchType();
        assertEquals(expectedType, match.matchType().name(),
                "Case " + ec.id + ": expected match_type=" + expectedType
                        + " but got " + match.matchType());

        double expectedConf = ec.expectedConfidence();
        assertEquals(expectedConf, match.confidence().doubleValue(), 0.001,
                "Case " + ec.id + ": expected confidence=" + expectedConf
                        + " but got " + match.confidence());
    }

    private RuleMatch evaluateRules(SettlementLine line, List<Rule> rules) {
        Set<String> seenRrns = new HashSet<>();
        for (Rule rule : rules) {
            RuleMatch m = rule.evaluate(line, seenRrns);
            if (m.matched()) return m;
        }
        return RuleMatch.of(MatchType.UNKNOWN, 0.0, null);
    }

    // ── agent mode ────────────────────────────────────────────────────────────

    /**
     * Drives a scripted agent simulation: feeds llm_script steps through
     * ScriptedAnthropicClient, dispatches tool stubs, captures propose_resolution.
     * Returns estimated cost USD (0.0 for mock runs).
     */
    private double runAgentCase(EvalCase ec) throws Exception {
        ScriptedGrokClient mockLlm = new ScriptedGrokClient(ec.llmScript, JSON_MAPPER);

        String resolvedType = null;
        int    steps        = 0;
        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "user", "content", "Investigate case: " + ec.id));

        while (steps < appConfig.agent().maxSteps()) {
            MessagesResponse resp = mockLlm.createMessage(
                    new MessagesRequest("mock", 1024, "", messages, List.of()));

            List<Map<String, Object>> toolResults = new ArrayList<>();
            for (ContentBlock block : resp.content()) {
                if (!block.isToolUse()) continue;
                steps++;

                if ("propose_resolution".equals(block.name())) {
                    resolvedType = (String) block.input().get("resolution_type");
                }

                Object stubResult = stubResult(ec.stubs, block.name());
                toolResults.add(Map.of(
                        "type",        "tool_result",
                        "tool_use_id", block.id(),
                        "content",     JSON_MAPPER.writeValueAsString(stubResult)
                ));
            }

            if (!toolResults.isEmpty()) {
                messages.add(Map.of("role", "user", "content", toolResults));
            }

            if ("end_turn".equals(resp.stopReason()) || resolvedType != null) break;
        }

        assertNotNull(resolvedType,
                "Case " + ec.id + ": agent never called propose_resolution");
        assertEquals(ec.expectedResolutionType(), resolvedType,
                "Case " + ec.id + ": expected resolution=" + ec.expectedResolutionType()
                        + " got " + resolvedType);

        List<String> invoked = mockLlm.toolsInvoked();
        for (String required : ec.mustCallTools()) {
            assertTrue(invoked.contains(required),
                    "Case " + ec.id + ": required tool [" + required + "] not called. Called: " + invoked);
        }

        return 0.0; // mock cost
    }

    // ── regression gate ───────────────────────────────────────────────────────

    private void checkRegression(double accuracy, double avgCost) throws Exception {
        if (!Files.exists(BASELINE)) return;

        @SuppressWarnings("unchecked")
        Map<String, Object> baseline = JSON_MAPPER.readValue(BASELINE.toFile(), Map.class);
        double prevAccuracy = ((Number) baseline.getOrDefault("accuracy",     1.0)).doubleValue();
        double prevCost     = ((Number) baseline.getOrDefault("avg_cost_usd", 0.0)).doubleValue();

        if (accuracy < prevAccuracy - 0.02) {
            fail(String.format("Accuracy regression: %.3f → %.3f (allowed drop: 0.02)",
                    prevAccuracy, accuracy));
        }
        if (prevCost > 0 && avgCost > prevCost * 1.20) {
            fail(String.format("Cost spike: $%.5f → $%.5f (allowed increase: 20%%)",
                    prevCost, avgCost));
        }
    }

    private void writeBaseline(double accuracy, double rulesAccuracy,
                                double agentAccuracy, double avgCost) throws Exception {
        Map<String, Object> baseline = new LinkedHashMap<>();
        baseline.put("accuracy",       accuracy);
        baseline.put("rules_accuracy", rulesAccuracy);
        baseline.put("agent_accuracy", agentAccuracy);
        baseline.put("avg_cost_usd",   avgCost);
        JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValue(BASELINE.toFile(), baseline);
    }

    // ── parsing helpers ───────────────────────────────────────────────────────

    private List<EvalCase> loadCases() throws Exception {
        List<EvalCase> cases = new ArrayList<>();
        try (Stream<Path> files = Files.list(CASES_DIR)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".yaml"))
                               .sorted().toList()) {
                cases.add(YAML_MAPPER.readValue(f.toFile(), EvalCase.class));
            }
        }
        return cases;
    }

    private SettlementLine parseSettlementLine(Map<String, Object> m) {
        return new SettlementLine(
                null,
                (String) m.get("file_id"),
                ((Number) m.get("line_no")).intValue(),
                (String) m.get("rrn"),
                (String) m.get("utr"),
                ((Number) m.get("amount_paise")).longValue(),
                m.containsKey("fee_paise") ? ((Number) m.get("fee_paise")).longValue() : 0L,
                m.containsKey("net_paise") ? ((Number) m.get("net_paise")).longValue() : 0L,
                (String) m.get("status"),
                null,
                Instant.now()
        );
    }

    private List<PgTransaction> parsePgTxns(List<Map<String, Object>> rows) {
        return rows.stream().map(r -> new PgTransaction(
                ((Number) r.get("id")).longValue(),
                (String) r.get("txn_id"),
                (String) r.get("rrn"),
                (String) r.get("utr"),
                (String) r.get("payer_vpa"),
                (String) r.get("payee_vpa"),
                ((Number) r.get("amount_paise")).longValue(),
                TxnStatus.valueOf((String) r.get("status")),
                parseInstant((String) r.get("created_at")),
                r.containsKey("updated_at")
                        ? parseInstant((String) r.get("updated_at"))
                        : Instant.now()
        )).toList();
    }

    private static Instant parseInstant(String s) {
        if (s == null) return Instant.now();
        // YAML values may lack the trailing Z
        return s.endsWith("Z") ? Instant.parse(s) : Instant.parse(s + "Z");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> stubList(Map<String, Object> stubs, String key) {
        Object v = stubs != null ? stubs.get(key) : null;
        return v instanceof List ? (List<Map<String, Object>>) v : List.of();
    }

    private Object stubResult(Map<String, Object> stubs, String toolName) {
        if (stubs == null) return List.of();
        Object v = stubs.get(toolName);
        if (v != null) return v;
        // YAML uses fetch_settlement_history_by_rrn; tool is named get_settlement_history
        if ("get_settlement_history".equals(toolName)) {
            v = stubs.get("fetch_settlement_history_by_rrn");
        }
        return v != null ? v : List.of();
    }

    // ── rules wiring ──────────────────────────────────────────────────────────

    private List<Rule> buildRules(EvalPgTransactionRepository repo) {
        return List.of(
                new ExactRrnRule(repo),
                new UtrAmountRule(repo),
                new ToleranceRule(repo, appConfig),
                new DuplicateRule(),
                new AmountMismatchRule(repo),
                new MissingLegRule(repo)
        );
    }
}
