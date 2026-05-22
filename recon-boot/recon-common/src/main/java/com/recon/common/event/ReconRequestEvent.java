package com.recon.common.event;

public record ReconRequestEvent(String caseUid, String fileId, int lineNo) {}
