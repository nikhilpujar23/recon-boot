package com.recon.rag;

import com.recon.common.config.AppConfig;
import com.recon.common.event.CaseApprovedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "recon.rag.enabled", havingValue = "true", matchIfMissing = false)
public class CaseApprovedEventListener {

    private static final Logger log = LoggerFactory.getLogger(CaseApprovedEventListener.class);

    private final CaseEmbeddingService embeddingService;
    private final AppConfig config;

    public CaseApprovedEventListener(CaseEmbeddingService embeddingService, AppConfig config) {
        this.embeddingService = embeddingService;
        this.config = config;
    }

    @Async
    @EventListener
    public void onCaseApproved(CaseApprovedEvent event) {
        if (!config.rag().enabled()) return;
        try {
            embeddingService.index(event.caseUid());
        } catch (Exception e) {
            // Non-fatal: gap-fill job will pick this up within indexJobIntervalMs
            log.warn("rag.index.failed case_uid={} error={}", event.caseUid(), e.getMessage());
        }
    }
}
