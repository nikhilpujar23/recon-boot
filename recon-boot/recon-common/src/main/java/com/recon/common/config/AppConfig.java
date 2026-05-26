package com.recon.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "recon")
public record AppConfig(
        Agent agent,
        Rules rules,
        Pii pii,
        Api api,
        Retry retry
) {
    public record Agent(
            String modelTriage,
            String modelInvest,
            int maxSteps,
            int timeoutSeconds,
            boolean mockLlm,
            int interCallDelayMs
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

}
