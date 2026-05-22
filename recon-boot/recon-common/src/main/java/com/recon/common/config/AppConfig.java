package com.recon.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "recon")
public record AppConfig(
        Agent agent,
        Rules rules,
        Pii pii,
        Api api,
        Retry retry,
        Rag rag
) {
    public record Agent(
            String modelTriage,
            String modelInvest,
            int maxSteps,
            int timeoutSeconds,
            boolean mockLlm
    ) {}

    public record Rules(
            long tolerancePaise
    ) {}

    public record Pii(
            String panHmacKey,
            String panAesKey
    ) {}

    public record Api(
            String bearerToken,
            int rateLimitRpm
    ) {}

    public record Retry(
            double initialSeconds,
            int maxAttempts,
            double multiplier,
            double maxSeconds
    ) {}

    public record Rag(
            boolean enabled,
            int topK,
            double minScore,
            boolean filterByMatchType,
            long indexJobIntervalMs
    ) {
        public static Rag defaults() {
            return new Rag(true, 3, 0.75, true, 300_000L);
        }
    }
}
