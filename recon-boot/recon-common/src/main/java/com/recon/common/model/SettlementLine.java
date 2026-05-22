package com.recon.common.model;

import java.time.Instant;
import java.util.Map;

public record SettlementLine(
        Long id,
        String fileId,
        int lineNo,
        String rrn,
        String utr,
        long amountPaise,
        long feePaise,
        long netPaise,
        String status,
        Map<String, Object> raw,
        Instant ingestedAt
) {}
