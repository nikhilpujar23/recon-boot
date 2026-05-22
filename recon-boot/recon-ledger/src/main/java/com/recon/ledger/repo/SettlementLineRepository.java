package com.recon.ledger.repo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.common.model.SettlementLine;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class SettlementLineRepository {

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public SettlementLineRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void insertIgnoreDuplicate(SettlementLine line, byte[] outboxPayload, String partitionKey) {
        // Atomically insert settlement line + outbox entry.
        // ON CONFLICT DO NOTHING guarantees idempotent replay; outbox only written when line is new.
        int inserted = jdbc.sql("""
                INSERT INTO settlement_lines
                    (file_id, line_no, rrn, utr, amount_paise, fee_paise, net_paise, status, raw, ingested_at)
                VALUES (:fileId, :lineNo, :rrn, :utr, :amountPaise, :feePaise, :netPaise, :status,
                        CAST(:raw AS jsonb), :ingestedAt)
                ON CONFLICT (file_id, line_no) DO NOTHING
                """)
                .param("fileId", line.fileId())
                .param("lineNo", line.lineNo())
                .param("rrn", line.rrn())
                .param("utr", line.utr())
                .param("amountPaise", line.amountPaise())
                .param("feePaise", line.feePaise())
                .param("netPaise", line.netPaise())
                .param("status", line.status())
                .param("raw", toJson(line.raw()))
                .param("ingestedAt", Timestamp.from(line.ingestedAt() != null ? line.ingestedAt() : Instant.now()))
                .update();

        if (inserted > 0) {
            jdbc.sql("""
                    INSERT INTO outbox (topic, partition_key, payload, schema_id)
                    VALUES ('recon.requests', :partitionKey, :payload, 2)
                    """)
                    .param("partitionKey", partitionKey)
                    .param("payload", outboxPayload)
                    .update();
        }
    }

    public Optional<SettlementLine> findById(long id) {
        return jdbc.sql("SELECT * FROM settlement_lines WHERE id = :id")
                .param("id", id)
                .query(this::map)
                .optional();
    }

    public Optional<SettlementLine> findByFileIdAndLineNo(String fileId, int lineNo) {
        return jdbc.sql("SELECT * FROM settlement_lines WHERE file_id = :fileId AND line_no = :lineNo")
                .param("fileId", fileId)
                .param("lineNo", lineNo)
                .query(this::map)
                .optional();
    }

    public List<SettlementLine> findByRrn(String rrn) {
        return jdbc.sql("SELECT * FROM settlement_lines WHERE rrn = :rrn ORDER BY ingested_at")
                .param("rrn", rrn)
                .query(this::map)
                .list();
    }

    private String toJson(Object obj) {
        if (obj == null) return "{}";
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }

    private SettlementLine map(ResultSet rs, int rowNum) throws SQLException {
        return new SettlementLine(
                rs.getLong("id"),
                rs.getString("file_id"),
                rs.getInt("line_no"),
                rs.getString("rrn"),
                rs.getString("utr"),
                rs.getLong("amount_paise"),
                rs.getLong("fee_paise"),
                rs.getLong("net_paise"),
                rs.getString("status"),
                null, // raw JSONB omitted from default mapping
                rs.getTimestamp("ingested_at").toInstant()
        );
    }
}
