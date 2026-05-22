package com.recon.rag;

import com.recon.common.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnProperty(name = "recon.rag.enabled", havingValue = "true", matchIfMissing = false)
public class CaseEmbeddingIndexJob {

    private static final Logger log = LoggerFactory.getLogger(CaseEmbeddingIndexJob.class);
    private static final int BATCH = 100;

    private final CaseEmbeddingService embeddingService;
    private final AppConfig config;
    private final org.springframework.jdbc.core.simple.JdbcClient jdbc;

    public CaseEmbeddingIndexJob(CaseEmbeddingService embeddingService,
                                  AppConfig config,
                                  org.springframework.jdbc.core.simple.JdbcClient jdbc) {
        this.embeddingService = embeddingService;
        this.config = config;
        this.jdbc = jdbc;
    }

    @Scheduled(fixedDelayString = "${recon.rag.index-job-interval-ms:300000}")
    public void run() {
        if (!config.rag().enabled()) return;

        List<String> missing = jdbc.sql("""
                SELECT rc.case_uid::text
                FROM recon_cases rc
                LEFT JOIN case_embeddings ce ON ce.case_uid = rc.case_uid
                WHERE rc.resolution IN ('APPROVED', 'AUTO_RESOLVED')
                  AND ce.case_uid IS NULL
                ORDER BY rc.updated_at
                LIMIT :batch
                """)
                .param("batch", BATCH)
                .query(String.class)
                .list();

        if (missing.isEmpty()) return;
        log.info("rag.gap_fill found={}", missing.size());

        for (String caseUid : missing) {
            try {
                embeddingService.index(caseUid);
            } catch (Exception e) {
                log.warn("rag.gap_fill.failed case_uid={} error={}", caseUid, e.getMessage());
            }
        }
    }
}
