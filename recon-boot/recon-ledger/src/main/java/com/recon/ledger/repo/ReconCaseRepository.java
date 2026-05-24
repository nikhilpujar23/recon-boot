package com.recon.ledger.repo;

import com.recon.common.messaging.ProtoCodec;
import com.recon.common.model.MatchType;
import com.recon.common.model.ReconCase;
import com.recon.common.model.Resolution;
import com.recon.v1.ReconInvestigate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ReconCaseRepository {

    private final JdbcClient jdbc;

    public ReconCaseRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public void upsertAutoResolved(String caseUid, long settlementLineId, Long pgTxnId,
                                   MatchType matchType, BigDecimal confidence, String npciStatus) {
        jdbc.sql("""
                INSERT INTO recon_cases
                    (case_uid, settlement_line, pg_transaction, match_type, resolution, confidence, resolved_by, npci_status)
                VALUES (:uid, :slId, :txnId, :matchType, 'AUTO_RESOLVED', :confidence, 'rules', :npciStatus)
                ON CONFLICT (case_uid) DO NOTHING
                """)
                .param("uid", UUID.fromString(caseUid))
                .param("slId", settlementLineId)
                .param("txnId", pgTxnId)
                .param("matchType", matchType.name())
                .param("confidence", confidence)
                .param("npciStatus", npciStatus)
                .update();
    }

    @Transactional
    public void upsertPending(String caseUid, long settlementLineId, MatchType matchType, String npciStatus) {
        int inserted = jdbc.sql("""
                INSERT INTO recon_cases
                    (case_uid, settlement_line, match_type, resolution, npci_status)
                VALUES (:uid, :slId, :matchType, 'PENDING', :npciStatus)
                ON CONFLICT (case_uid) DO NOTHING
                """)
                .param("uid", UUID.fromString(caseUid))
                .param("slId", settlementLineId)
                .param("matchType", matchType.name())
                .param("npciStatus", npciStatus)
                .update();

        // Atomically write the investigate outbox entry so agent-worker can poll
        // it across the JVM boundary. Only written when the case is new (inserted > 0)
        // to preserve idempotency on replay.
        if (inserted > 0) {
            byte[] payload = ProtoCodec.encode(
                    ReconInvestigate.newBuilder().setCaseUid(caseUid).build());
            jdbc.sql("""
                    INSERT INTO outbox (topic, partition_key, payload, schema_id)
                    VALUES ('recon.investigate', :key, :payload, 3)
                    """)
                    .param("key", caseUid)
                    .param("payload", payload)
                    .update();
        }
    }

    /**
     * Agent write guard: only updates if resolution IS PENDING (prevents stale writes).
     * Returns true if the row was updated.
     */
    @Transactional
    public boolean proposeResolution(String caseUid, Resolution resolution, BigDecimal confidence,
                                     String resolvedBy, Long pgTxnId, String notes) {
        int rows = jdbc.sql("""
                UPDATE recon_cases
                SET resolution = :resolution,
                    confidence = :confidence,
                    resolved_by = :resolvedBy,
                    pg_transaction = :txnId,
                    notes = CAST(:notes AS jsonb),
                    resolved_at = now()
                WHERE case_uid = :uid AND resolution = 'PENDING'
                """)
                .param("resolution", resolution.name())
                .param("confidence", confidence)
                .param("resolvedBy", resolvedBy)
                .param("txnId", pgTxnId)
                .param("notes", notes != null ? notes : "{}")
                .param("uid", UUID.fromString(caseUid))
                .update();
        return rows > 0;
    }

    @Transactional
    public boolean approve(String caseUid, String reviewerEmail, String comment) {
        int rows = jdbc.sql("""
                UPDATE recon_cases
                SET resolution = 'APPROVED',
                    resolved_by = :reviewer,
                    notes = jsonb_set(COALESCE(notes,'{}'), '{comment}', to_jsonb(:comment::text)),
                    resolved_at = now()
                WHERE case_uid = :uid AND resolution IN ('PENDING', 'PROPOSED')
                """)
                .param("reviewer", reviewerEmail)
                .param("comment", comment != null ? comment : "")
                .param("uid", UUID.fromString(caseUid))
                .update();
        return rows > 0;
    }

    @Transactional
    public boolean reject(String caseUid, String reviewerEmail, String comment) {
        int rows = jdbc.sql("""
                UPDATE recon_cases
                SET resolution = 'REJECTED',
                    resolved_by = :reviewer,
                    notes = jsonb_set(COALESCE(notes,'{}'), '{comment}', to_jsonb(:comment::text)),
                    resolved_at = now()
                WHERE case_uid = :uid AND resolution = 'PROPOSED'
                """)
                .param("reviewer", reviewerEmail)
                .param("comment", comment != null ? comment : "")
                .param("uid", UUID.fromString(caseUid))
                .update();
        return rows > 0;
    }

    public Optional<ReconCase> findByCaseUid(String caseUid) {
        return jdbc.sql("SELECT * FROM recon_cases WHERE case_uid = :uid")
                .param("uid", UUID.fromString(caseUid))
                .query(this::map)
                .optional();
    }

    public List<ReconCase> findPage(Resolution resolution, MatchType matchType,
                                    long cursor, int limit) {
        String filter = "";
        if (resolution != null) filter += " AND resolution = '" + resolution.name() + "'";
        if (matchType != null)  filter += " AND match_type = '" + matchType.name() + "'";

        return jdbc.sql("SELECT * FROM recon_cases WHERE id > :cursor" + filter
                        + " ORDER BY id LIMIT :limit")
                .param("cursor", cursor)
                .param("limit", Math.min(limit, 200))
                .query(this::map)
                .list();
    }

    private ReconCase map(ResultSet rs, int rowNum) throws SQLException {
        String resStr = rs.getString("resolution");
        String mtStr  = rs.getString("match_type");
        return new ReconCase(
                rs.getLong("id"),
                rs.getString("case_uid"),
                rs.getLong("settlement_line"),
                rs.getObject("pg_transaction") != null ? rs.getLong("pg_transaction") : null,
                mtStr != null ? MatchType.valueOf(mtStr) : null,
                resStr != null ? Resolution.valueOf(resStr) : null,
                rs.getBigDecimal("confidence"),
                rs.getString("resolved_by"),
                rs.getString("npci_status"),
                null,
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("resolved_at") != null ? rs.getTimestamp("resolved_at").toInstant() : null
        );
    }
}
