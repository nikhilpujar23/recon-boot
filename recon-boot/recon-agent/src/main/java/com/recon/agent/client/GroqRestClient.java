package com.recon.agent.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.util.*;

/**
 * Thin wrapper around the Groq OpenAI-compatible Chat Completions API.
 * Used only by the eval harness (ScriptedGroqClient).
 * Production code uses LangChain4j's OpenAiChatModel instead.
 */
public class GroqRestClient {

    private static final String API_URL = "https://api.groq.com/openai/v1";

    private final RestClient   http;
    private final ObjectMapper json;
    private final boolean      mockLlm;

    public GroqRestClient(
            @Value("${GROQ_API_KEY:}") String apiKey,
            ObjectMapper json,
            @Value("${recon.agent.mock-llm:false}") boolean mockLlm) {

        this.json    = json;
        this.mockLlm = mockLlm;
        this.http = RestClient.builder()
                .baseUrl(API_URL)
                .defaultHeader("Authorization",  "Bearer " + apiKey)
                .defaultHeader("Content-Type",   "application/json")
                .build();
    }

    @SuppressWarnings("unchecked")
    public MessagesResponse createMessage(MessagesRequest request) {
        if (mockLlm) {
            return mockResponse();
        }

        // Build OpenAI-compatible request body
        List<Map<String, Object>> messages = new ArrayList<>();
        if (request.system() != null && !request.system().isBlank()) {
            messages.add(Map.of("role", "system", "content", request.system()));
        }
        messages.addAll(request.messages());

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model",      request.model());
        body.put("max_tokens", request.max_tokens());
        body.put("messages",   messages);
        if (request.tools() != null && !request.tools().isEmpty()) {
            body.put("tools", request.tools());
        }

        String responseBody = http.post()
                .uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .body(String.class);
        try {
            Map<String, Object> raw = json.readValue(responseBody, Map.class);
            return parseResponse(raw);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Groq response: " + responseBody, e);
        }
    }

    // ── Request / Response records ────────────────────────────────────────────

    public record MessagesRequest(
            String model,
            int max_tokens,
            String system,
            List<Map<String, Object>> messages,
            List<Map<String, Object>> tools
    ) {}

    public record MessagesResponse(
            String stopReason,
            List<ContentBlock> content,
            Usage usage
    ) {}

    public record ContentBlock(
            String type,
            String id,
            String name,
            String text,
            Map<String, Object> input
    ) {
        public boolean isToolUse() { return "tool_use".equals(type); }
        public boolean isText()    { return "text".equals(type);     }
    }

    public record Usage(int inputTokens, int outputTokens,
                        int cacheReadTokens, int cacheCreationTokens) {
        public double estimateCostUsd() {
            // Groq free tier — cost is $0; keep the field for baseline compatibility
            return 0.0;
        }
    }

    // ── parsing ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private MessagesResponse parseResponse(Map<String, Object> raw) throws Exception {
        List<Map<String, Object>> choices =
                (List<Map<String, Object>>) raw.getOrDefault("choices", List.of());
        Map<String, Object> rawUsage =
                (Map<String, Object>) raw.getOrDefault("usage", Map.of());

        String stopReason = "end_turn";
        List<ContentBlock> blocks = new ArrayList<>();

        if (!choices.isEmpty()) {
            Map<String, Object> choice  = choices.get(0);
            String finishReason         = (String) choice.getOrDefault("finish_reason", "stop");
            Map<String, Object> message = (Map<String, Object>) choice.getOrDefault("message", Map.of());

            stopReason = "tool_calls".equals(finishReason) ? "tool_use" : "end_turn";

            String text = (String) message.get("content");
            if (text != null && !text.isBlank()) {
                blocks.add(new ContentBlock("text", null, null, text, Map.of()));
            }

            List<Map<String, Object>> toolCalls =
                    (List<Map<String, Object>>) message.getOrDefault("tool_calls", List.of());
            for (Map<String, Object> tc : toolCalls) {
                String id = (String) tc.get("id");
                Map<String, Object> fn   = (Map<String, Object>) tc.get("function");
                String name              = (String) fn.get("name");
                String argsJson          = (String) fn.get("arguments");
                Map<String, Object> args = argsJson != null
                        ? json.readValue(argsJson, Map.class)
                        : Map.of();
                blocks.add(new ContentBlock("tool_use", id, name, null, args));
            }
        }

        Usage usage = new Usage(
                ((Number) rawUsage.getOrDefault("prompt_tokens",     0)).intValue(),
                ((Number) rawUsage.getOrDefault("completion_tokens", 0)).intValue(),
                0, 0
        );
        return new MessagesResponse(stopReason, blocks, usage);
    }

    // ── mock ──────────────────────────────────────────────────────────────────

    private MessagesResponse mockResponse() {
        return new MessagesResponse("end_turn", List.of(), new Usage(0, 0, 0, 0));
    }
}
