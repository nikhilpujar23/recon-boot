package com.recon.common.event;

/** Published after a case transitions to APPROVED or AUTO_RESOLVED. */
public record CaseApprovedEvent(String caseUid, Long pgTransactionId, String npciStatus) {}
