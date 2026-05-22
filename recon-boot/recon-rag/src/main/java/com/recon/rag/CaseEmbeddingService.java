package com.recon.rag;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@ConditionalOnProperty(name = "recon.rag.enabled", havingValue = "true", matchIfMissing = false)
public class CaseEmbeddingService {

    private static final Logger log = LoggerFactory.getLogger(CaseEmbeddingService.class);

    private final JdbcClient jdbc;
    private final EmbeddingModel embeddingModel;

    public CaseEmbeddingService(JdbcClient jdbc, EmbeddingModel embeddingModel) {
        this.jdbc = jdbc;
        this.embeddingModel = embeddingModel;
    }

    public void index(String caseUid) {
        Optional<CaseSnapshot> snapshot = fetchSnapshot(caseUid);
        if (snapshot.isEmpty()) {
            log.warn("rag.index.skip case_uid={} reason=snapshot_not_found", caseUid);
            return;
        }
        CaseSnapshot s = snapshot.get();
        String content = buildContent(s);
        Embedding embedding = embeddingModel.embed(content).content();
        String vectorLiteral = VectorUtils.toLiteral(embedding.vector());
        upsert(s, content, vectorLiteral);
        log.info("rag.indexed case_uid={} match_type={} resolution={}", caseUid, s.matchType(), s.resolution());
    }

    private Optional<CaseSnapshot> fetchSnapshot(String caseUid) {
        return jdbc.sql("""
                SELECT
                    rc.case_uid,
                    rc.match_type,
                    rc.resolution,
                    COALESCE(rc.confidence, 0)         AS confidence,
                    sl.status                           AS settlement_status,
                    COALESCE(sl.amount_paise, 0)        AS settlement_amount_paise,
                    pt.status                           AS pg_status,
                    COALESCE(pt.amount_paise, 0)        AS pg_amount_paise,
                    at.tools_used,
                    rc.notes ->> 'rationale'            AS rationale
                FROM recon_cases rc
                LEFT JOIN settlement_lines sl  ON sl.id  = rc.settlement_line
                LEFT JOIN pg_transactions  pt  ON pt.id  = rc.pg_transaction
                LEFT JOIN LATERAL (
                    SELECT tools_used FROM agent_traces
                    WHERE case_uid = rc.case_uid
                    ORDER BY created_at DESC LIMIT 1
                ) at ON true
                WHERE rc.case_uid = :uid
                  AND rc.resolution IN ('APPROVED', 'AUTO_RESOLVED')
                """)
                .param("uid", UUID.fromString(caseUid))
                .query(this::mapSnapshot)
                .optional();
    }

    private CaseSnapshot mapSnapshot(ResultSet rs, int row) throws SQLException {
        String[] toolsArr = (String[]) (rs.getArray("tools_used") != null
                ? rs.getArray("tools_used").getArray() : new String[0]);
        return new CaseSnapshot(
                rs.getString("case_uid"),
                rs.getString("match_type"),
                rs.getString("resolution"),
                rs.getDouble("confidence"),
                rs.getString("settlement_status"),
                rs.getLong("settlement_amount_paise"),
                rs.getString("pg_status"),
                rs.getLong("pg_amount_paise"),
                Arrays.asList(toolsArr),
                rs.getString("rationale")
        );
    }

    private String buildContent(CaseSnapshot s) {
        return """
                case_uid: %s
                match_type: %s
                resolution: %s
                confidence: %.2f
                settlement_status: %s  settlement_amount_paise: %d
                pg_status: %s  pg_amount_paise: %d
                tools_used: %s
                rationale: %s
                """.formatted(
                s.caseUid(),
                s.matchType(),
                s.resolution(),
                s.confidence(),
                s.settlementStatus(),
                s.settlementAmountPaise(),
                s.pgStatus(),
                s.pgAmountPaise(),
                String.join(", ", s.toolsUsed()),
                s.rationale() != null ? s.rationale() : ""
        );
    }

    private void upsert(CaseSnapshot s, String content, String vectorLiteral) {
        jdbc.sql("""
                INSERT INTO case_embeddings
                    (case_uid, match_type, resolution, content, embedding)
                VALUES (:uid, :matchType, :resolution, :content, CAST(:embedding AS vector))
                ON CONFLICT (case_uid) DO UPDATE
                    SET match_type  = EXCLUDED.match_type,
                        resolution  = EXCLUDED.resolution,
                        content     = EXCLUDED.content,
                        embedding   = EXCLUDED.embedding,
                        indexed_at  = now()
                """)
                .param("uid", UUID.fromString(s.caseUid()))
                .param("matchType", s.matchType())
                .param("resolution", s.resolution())
                .param("content", content)
                .param("embedding", vectorLiteral)
                .update();
    }
}
