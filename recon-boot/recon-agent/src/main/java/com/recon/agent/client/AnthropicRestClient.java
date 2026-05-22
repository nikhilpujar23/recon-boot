package com.recon.agent.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;

/**
 * Thin wrapper around the Anthropic Messages REST API.
 * Used only by the eval harness (ScriptedAnthropicClient).
 * Production code uses LangChain4j's AnthropicChatModel instead.
 */
public class AnthropicRestClient {

    private static final String API_URL     = "https://api.anthropic.com";
    private static final String API_VERSION = "2023-06-01";

    private final RestClient    http;
    private final ObjectMapper  json;
    private final boolean       mockLlm;

    public AnthropicRestClient(
            @Value("${ANTHROPIC_API_KEY:}") String apiKey,
            ObjectMapper json,
            @Value("${recon.agent.mock-llm:false}") boolean mockLlm) {

        this.json    = json;
        this.mockLlm = mockLlm;
        this.http = RestClient.builder()
                .baseUrl(API_URL)
                .defaultHeader("x-api-key",         apiKey)
                .defaultHeader("anthropic-version",  API_VERSION)
                .defaultHeader("Content-Type",       "application/json")
                // prompt-caching beta header
                .defaultHeader("anthropic-beta",     "prompt-caching-2024-07-31")
                .build();
    }

    /**
     * Sends a messages request and returns the parsed response body as a Map.
     * Callers inspect response["stop_reason"] and response["content"].
     */
    @SuppressWarnings("unchecked")
    public MessagesResponse createMessage(MessagesRequest request) {
        if (mockLlm) {
            return mockResponse();
        }
        String body = http.post()
                .uri("/v1/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .body(String.class);
        try {
            Map<String, Object> raw = json.readValue(body, Map.class);
            return MessagesResponse.from(raw);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Anthropic response: " + body, e);
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
    ) {
        @SuppressWarnings("unchecked")
        static MessagesResponse from(Map<String, Object> raw) {
            String stopReason = (String) raw.getOrDefault("stop_reason", "end_turn");
            List<Map<String, Object>> rawContent =
                    (List<Map<String, Object>>) raw.getOrDefault("content", List.of());
            Map<String, Object> rawUsage =
                    (Map<String, Object>) raw.getOrDefault("usage", Map.of());

            List<ContentBlock> blocks = rawContent.stream().map(ContentBlock::from).toList();
            Usage usage = new Usage(
                    ((Number) rawUsage.getOrDefault("input_tokens",  0)).intValue(),
                    ((Number) rawUsage.getOrDefault("output_tokens", 0)).intValue(),
                    ((Number) rawUsage.getOrDefault("cache_read_input_tokens",    0)).intValue(),
                    ((Number) rawUsage.getOrDefault("cache_creation_input_tokens",0)).intValue()
            );
            return new MessagesResponse(stopReason, blocks, usage);
        }
    }

    public record ContentBlock(
            String type,        // "text" | "tool_use"
            String id,          // tool_use id
            String name,        // tool name
            String text,        // text content
            Map<String, Object> input   // tool arguments
    ) {
        @SuppressWarnings("unchecked")
        static ContentBlock from(Map<String, Object> raw) {
            return new ContentBlock(
                    (String) raw.get("type"),
                    (String) raw.get("id"),
                    (String) raw.get("name"),
                    (String) raw.get("text"),
                    raw.containsKey("input") ? (Map<String, Object>) raw.get("input") : Map.of()
            );
        }

        public boolean isToolUse() { return "tool_use".equals(type); }
        public boolean isText()    { return "text".equals(type);     }
    }

    public record Usage(int inputTokens, int outputTokens,
                        int cacheReadTokens, int cacheCreationTokens) {
        /** Rough cost estimate in USD (Sonnet pricing as baseline). */
        public double estimateCostUsd() {
            double inputCost  = inputTokens  * 3.0   / 1_000_000;
            double outputCost = outputTokens * 15.0  / 1_000_000;
            double cacheSaved = cacheReadTokens * 0.3 / 1_000_000;
            return inputCost + outputCost - cacheSaved;
        }
    }

    // ── mock ──────────────────────────────────────────────────────────────────

    private MessagesResponse mockResponse() {
        return new MessagesResponse("end_turn", List.of(), new Usage(0, 0, 0, 0));
    }
}
