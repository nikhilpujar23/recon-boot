package com.recon.common.model;

import java.time.Instant;

public record PgTransaction(
        Long id,
        String txnId,
        String rrn,
        String utr,
        String payerVpa,
        String payeeVpa,
        long amountPaise,
        TxnStatus status,
        Instant createdAt,
        Instant updatedAt
) {}
