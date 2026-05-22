package com.recon.common.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public record AgentTrace(
        Long id,
        String caseUid,
        String promptHash,
        String model,
        List<String> toolsUsed,
        List<Map<String, Object>> steps,
        long totalMs,
        BigDecimal costUsd,
        Instant createdAt
) {}
