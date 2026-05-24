package com.recon.eval;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.agent.client.GroqRestClient;

import java.util.*;

/**
 * Deterministic mock LLM client driven by an llm_script list from a YAML case.
 *
 * Each step in the script is either:
 *   {call: "tool_name", args: {...}}  → returns a tool_use ContentBlock
 *   {propose: {resolution_type, confidence, rationale, pg_txn_id?}}  → returns propose_resolution tool_use
 *
 * After the script is exhausted, returns end_turn with no content.
 */
class ScriptedAnthropicClient extends GroqRestClient {

    private final Deque<Map<String, Object>> steps;
    private final List<String> toolsInvoked = new ArrayList<>();

    ScriptedAnthropicClient(List<Map<String, Object>> llmScript, ObjectMapper json) {
        super("", json, false);
        this.steps = new ArrayDeque<>(llmScript);
    }

    @Override
    public MessagesResponse createMessage(MessagesRequest request) {
        if (steps.isEmpty()) {
            return new MessagesResponse("end_turn", List.of(), new Usage(0, 0, 0, 0));
        }

        Map<String, Object> step = steps.poll();

        if (step.containsKey("call")) {
            String toolName = (String) step.get("call");
            @SuppressWarnings("unchecked")
            Map<String, Object> args = step.containsKey("args")
                    ? (Map<String, Object>) step.get("args")
                    : Map.of();

            toolsInvoked.add(toolName);
            ContentBlock block = new ContentBlock(
                    "tool_use",
                    "tool_" + toolsInvoked.size(),
                    toolName,
                    null,
                    args
            );
            return new MessagesResponse("tool_use", List.of(block), new Usage(100, 50, 0, 0));
        }

        if (step.containsKey("propose")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> propose = (Map<String, Object>) step.get("propose");
            Map<String, Object> proposeInput = new LinkedHashMap<>(propose);

            toolsInvoked.add("propose_resolution");
            ContentBlock block = new ContentBlock(
                    "tool_use",
                    "tool_propose",
                    "propose_resolution",
                    null,
                    proposeInput
            );
            steps.clear();
            return new MessagesResponse("tool_use", List.of(block), new Usage(200, 100, 0, 0));
        }

        return new MessagesResponse("end_turn", List.of(), new Usage(0, 0, 0, 0));
    }

    List<String> toolsInvoked() { return Collections.unmodifiableList(toolsInvoked); }
}
