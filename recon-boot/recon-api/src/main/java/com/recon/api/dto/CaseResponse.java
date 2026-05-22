package com.recon.api.dto;

import com.recon.common.model.MatchType;
import com.recon.common.model.ReconCase;
import com.recon.common.model.Resolution;

import java.math.BigDecimal;
import java.time.Instant;

public record CaseResponse(
        String caseUid,
        Long settlementLineId,
        Long pgTransactionId,
        MatchType matchType,
        Resolution resolution,
        BigDecimal confidence,
        String resolvedBy,
        Instant createdAt,
        Instant updatedAt
) {
    public static CaseResponse from(ReconCase c) {
        return new CaseResponse(c.caseUid(), c.settlementLineId(), c.pgTransactionId(),
                c.matchType(), c.resolution(), c.confidence(), c.resolvedBy(),
                c.createdAt(), c.updatedAt());
    }
}
