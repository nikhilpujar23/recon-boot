package com.recon.rag;

import java.util.List;

/** Data fetched from the DB to build the embedding content text for a resolved case. */
record CaseSnapshot(
        String       caseUid,
        String       matchType,
        String       resolution,
        double       confidence,
        String       settlementStatus,
        long         settlementAmountPaise,
        String       pgStatus,
        long         pgAmountPaise,
        List<String> toolsUsed,
        String       rationale
) {}
