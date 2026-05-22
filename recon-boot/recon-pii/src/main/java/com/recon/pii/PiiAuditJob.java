package com.recon.pii;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Nightly job (LLD §14 — "Security: PII audit | nightly cron + grep | zero unmasked findings").
 *
 * Scans text columns that could inadvertently store raw PII and fires an alert metric on any hit.
 * Fail-closed design: if the redactor says a value contains PII, it is a finding regardless of
 * whether redaction was supposed to have run upstream.
 */
@Component
public class PiiAuditJob {

    private static final Logger log = LoggerFactory.getLogger(PiiAuditJob.class);

    private final JdbcClient   jdbc;
    private final PiiRedactor  redactor;
    private final Counter      findingsCounter;

    // Columns to audit: table → list of column names holding free-form text
    private static final Map<String, List<String>> AUDIT_TARGETS = Map.of(
            "pg_transactions",  List.of("payer_vpa", "payee_vpa"),
            "settlement_lines", List.of("rrn", "utr"),
            "agent_traces",     List.of("steps")   // JSONB stored as text
    );

    public PiiAuditJob(JdbcClient jdbc, PiiRedactor redactor, MeterRegistry registry) {
        this.jdbc     = jdbc;
        this.redactor = redactor;
        this.findingsCounter = Counter.builder("recon_pii_audit_findings_total")
                .description("Number of unmasked PII values detected during nightly audit")
                .register(registry);
    }

    @Scheduled(cron = "${recon.pii.audit-cron:0 0 2 * * *}")
    public void run() {
        log.info("PII audit starting");
        int totalFindings = 0;

        for (Map.Entry<String, List<String>> entry : AUDIT_TARGETS.entrySet()) {
            String table   = entry.getKey();
            List<String> cols = entry.getValue();
            for (String col : cols) {
                int findings = auditColumn(table, col);
                totalFindings += findings;
            }
        }

        if (totalFindings > 0) {
            // ERROR level triggers the RedactionFailure alert (LLD §13)
            log.error("PII audit FAILED — {} unmasked value(s) found across monitored columns. "
                      + "Inspect logs above for table/column details.", totalFindings);
        } else {
            log.info("PII audit PASSED — zero unmasked findings");
        }
    }

    private int auditColumn(String table, String col) {
        List<String> values = jdbc
                .sql("SELECT " + col + "::text FROM " + table + " WHERE " + col + " IS NOT NULL")
                .query((rs, n) -> rs.getString(1))
                .list();

        int count = 0;
        for (String value : values) {
            if (redactor.containsPii(value)) {
                count++;
                findingsCounter.increment();
                log.error("Unmasked PII detected — table={} col={} preview={}",
                        table, col, sanitizeForLog(value));
            }
        }
        return count;
    }

    // Truncate the value so we don't log the full PII in the audit trail itself
    private static String sanitizeForLog(String value) {
        if (value == null) return "<null>";
        return value.length() > 12 ? value.substring(0, 6) + "..." : value;
    }
}
