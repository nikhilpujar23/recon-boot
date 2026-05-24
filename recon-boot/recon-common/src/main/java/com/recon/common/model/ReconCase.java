package com.recon.common.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

public record ReconCase(
        Long id,
        String caseUid,
        Long settlementLineId,
        Long pgTransactionId,
        MatchType matchType,
        Resolution resolution,
        BigDecimal confidence,
        String resolvedBy,
        String npciStatus,
        Map<String, Object> notes,
        Instant createdAt,
        Instant updatedAt
) {}
