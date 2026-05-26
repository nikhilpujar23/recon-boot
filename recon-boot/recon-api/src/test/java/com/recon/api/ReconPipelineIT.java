package com.recon.api;

import com.recon.common.model.*;
import com.recon.common.util.CaseUid;
import com.recon.ingest.sftp.SftpWatcher;
import com.recon.ledger.outbox.OutboxDrainer;
import com.recon.ledger.repo.*;
import com.recon.rules.engine.RulesEngine;
import com.recon.v1.ReconRequest;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end pipeline integration tests: parser → DB → rules → outbox.
 * Uses Testcontainers Postgres.
 * Covers: happy path + 3 failure modes (duplicate ingestion, agent write guard, outbox idempotency).
 */
@SpringBootTest(classes = ReconApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@TestPropertySource(properties = {
        "recon.agent.mock-llm=true"
})
class ReconPipelineIT {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("recon")
                    .withUsername("recon")
                    .withPassword("recon");

    @DynamicPropertySource
    static void configureDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @MockBean SftpWatcher sftpWatcher;
    @MockBean OutboxDrainer outboxDrainer;

    @Autowired RulesEngine          rulesEngine;
    @Autowired SettlementLineRepository slRepo;
    @Autowired ReconCaseRepository  caseRepo;
    @Autowired ProcessedFileRepository fileRepo;
    @Autowired PgTransactionRepository pgTxnRepo;
    @Autowired JdbcClient           jdbc;

    // ── helpers ────────────────────────────────────────────────────────────────

    private long seedPgTransaction(String rrn, String utr, long amountPaise) {
        jdbc.sql("""
                INSERT INTO pg_transactions
                    (txn_id, rrn, utr, amount_paise, status, created_at, updated_at)
                VALUES (:txnId, :rrn, :utr, :amount, 'SUCCESS', :now, :now)
                """)
                .param("txnId", UUID.randomUUID().toString())
                .param("rrn", rrn)
                .param("utr", utr)
                .param("amount", amountPaise)
                .param("now", Timestamp.from(Instant.now()))
                .update();
        return jdbc.sql("SELECT id FROM pg_transactions WHERE rrn = :rrn")
                .param("rrn", rrn)
                .query((rs, n) -> rs.getLong("id"))
                .single();
    }

    private long seedSettlementLine(String fileId, int lineNo, String rrn, String utr,
                                    long amountPaise) {
        byte[] outboxPayload = ReconRequest.newBuilder()
                .setFileId(fileId).setLineNo(lineNo)
                .setCaseUid(CaseUid.of(fileId, lineNo))
                .build().toByteArray();
        SettlementLine line = new SettlementLine(
                0L, fileId, lineNo, rrn, utr, amountPaise, 0L, amountPaise,
                "SUCCESS", null, Instant.now());
        slRepo.insertIgnoreDuplicate(line, outboxPayload, rrn);
        return slRepo.findByFileIdAndLineNo(fileId, lineNo)
                .orElseThrow().id();
    }

    private void cleanTables() {
        jdbc.sql("DELETE FROM agent_traces").update();
        jdbc.sql("DELETE FROM recon_cases").update();
        jdbc.sql("DELETE FROM outbox").update();
        jdbc.sql("DELETE FROM settlement_lines").update();
        jdbc.sql("DELETE FROM processed_files").update();
        jdbc.sql("DELETE FROM pg_transactions").update();
    }

    @BeforeEach
    void setUp() {
        cleanTables();
    }

    // ── Test 1: happy path — exact RRN match → AUTO_RESOLVED ──────────────────

    @Test
    void happyPath_exactMatch_createsAutoResolvedCase() {
        String fileId = "FILE_HAPPY";
        String rrn    = "RRN_HAPPY_001";
        String utr    = "UTR_HAPPY_001";
        long   amount = 50_000L;

        seedPgTransaction(rrn, utr, amount);
        long slId = seedSettlementLine(fileId, 1, rrn, utr, amount);

        SettlementLine line = slRepo.findById(slId).orElseThrow();
        RulesEngine.RulesResult result = rulesEngine.processFile(fileId, List.of(line));

        assertEquals(1, result.autoResolved(), "Exact match should be auto-resolved");
        assertEquals(0, result.pendingAgent(),  "No cases should be pending");

        String caseUid = CaseUid.of(fileId, 1);
        Optional<ReconCase> caseOpt = caseRepo.findByCaseUid(caseUid);
        assertTrue(caseOpt.isPresent(), "Case must be persisted");
        assertEquals(Resolution.AUTO_RESOLVED, caseOpt.get().resolution());
        assertEquals(MatchType.EXACT,          caseOpt.get().matchType());
    }

    // ── Test 2: failure mode — duplicate file dedup (processed_files) ─────────

    @Test
    void failureMode_duplicateFile_secondSubmissionIsIdempotent() {
        String sha256   = "a".repeat(64);
        String fileId   = "FILE_DUP";
        String filename = "udir_20240115.txt";

        boolean first  = fileRepo.markSeen(sha256, fileId, filename, 1024L);
        boolean second = fileRepo.markSeen(sha256, fileId, filename, 1024L);

        assertTrue(first,   "First submission must be marked as new");
        assertFalse(second, "Duplicate SHA-256 must be rejected");
    }

    // ── Test 3: failure mode — agent write guard (stale resolution blocked) ───

    @Test
    void failureMode_agentWriteGuard_blocksStaleResolution() {
        String fileId = "FILE_GUARD";
        String rrn    = "RRN_GUARD_001";
        long   amount = 20_000L;

        seedPgTransaction(rrn, "UTR_GUARD_001", amount);
        long slId  = seedSettlementLine(fileId, 1, rrn, "UTR_GUARD_001", amount);
        String uid = CaseUid.of(fileId, 1);

        caseRepo.upsertPending(uid, slId, MatchType.AMOUNT_MISMATCH, null);

        boolean first = caseRepo.proposeResolution(
                uid, Resolution.PROPOSED, BigDecimal.valueOf(0.92),
                "agent:haiku", null, "{\"reason\":\"fee delta\"}");
        assertTrue(first, "First agent write must succeed");

        boolean second = caseRepo.proposeResolution(
                uid, Resolution.PROPOSED, BigDecimal.valueOf(0.75),
                "agent:sonnet", null, "{\"reason\":\"retry\"}");
        assertFalse(second, "Stale agent write must be blocked by write guard");

        ReconCase saved = caseRepo.findByCaseUid(uid).orElseThrow();
        assertEquals(0, BigDecimal.valueOf(0.92).compareTo(saved.confidence()),
                "Confidence must not be overwritten by stale write");
    }

    // ── Test 4: failure mode — outbox idempotency (duplicate line insert) ─────

    @Test
    void failureMode_outboxIdempotency_duplicateLineProducesOneOutboxEntry() {
        String fileId = "FILE_IDEM";
        String rrn    = "RRN_IDEM_001";
        long   amount = 15_000L;
        byte[] payload = ReconRequest.newBuilder()
                .setFileId(fileId).setLineNo(1)
                .setCaseUid(CaseUid.of(fileId, 1))
                .build().toByteArray();
        SettlementLine line = new SettlementLine(
                0L, fileId, 1, rrn, "UTR_IDEM", amount, 0L, amount,
                "SUCCESS", null, Instant.now());

        slRepo.insertIgnoreDuplicate(line, payload, rrn);
        slRepo.insertIgnoreDuplicate(line, payload, rrn);

        long slCount = jdbc.sql("SELECT COUNT(*) FROM settlement_lines WHERE file_id = :fid")
                .param("fid", fileId).query((rs, n) -> rs.getLong(1)).single();
        long outboxCount = jdbc.sql("SELECT COUNT(*) FROM outbox WHERE partition_key = :key")
                .param("key", rrn).query((rs, n) -> rs.getLong(1)).single();

        assertEquals(1L, slCount,     "Exactly one settlement_line row must exist");
        assertEquals(1L, outboxCount, "Exactly one outbox entry must exist");
    }
}
