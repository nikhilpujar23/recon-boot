package com.recon.ledger.repo;

import com.recon.common.model.AgentTrace;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class AgentTraceRepository {

    private final JdbcClient jdbc;

    public AgentTraceRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(String caseUid, String promptHash, String model,
                       List<String> toolsUsed, String stepsJson,
                       long totalMs, BigDecimal costUsd) {
        jdbc.sql("""
                INSERT INTO agent_traces
                    (case_uid, prompt_hash, model, tools_used, steps, total_ms, cost_usd)
                VALUES (:caseUid, :promptHash, :model,
                        CAST(:tools AS text[]), CAST(:steps AS jsonb),
                        :totalMs, :costUsd)
                """)
                .param("caseUid", UUID.fromString(caseUid))
                .param("promptHash", promptHash)
                .param("model", model)
                .param("tools", "{" + String.join(",", toolsUsed) + "}")
                .param("steps", stepsJson)
                .param("totalMs", totalMs)
                .param("costUsd", costUsd)
                .update();
    }

    public Optional<AgentTrace> findLatestByCaseUid(String caseUid) {
        return jdbc.sql("""
                SELECT * FROM agent_traces WHERE case_uid = :uid
                ORDER BY created_at DESC LIMIT 1
                """)
                .param("uid", UUID.fromString(caseUid))
                .query(this::map)
                .optional();
    }

    private AgentTrace map(ResultSet rs, int rowNum) throws SQLException {
        Array toolsArray = rs.getArray("tools_used");
        List<String> tools = toolsArray != null
                ? Arrays.asList((String[]) toolsArray.getArray())
                : List.of();
        return new AgentTrace(
                rs.getLong("id"),
                rs.getString("case_uid"),
                rs.getString("prompt_hash"),
                rs.getString("model"),
                tools,
                null, // steps deserialization handled upstream
                rs.getLong("total_ms"),
                rs.getBigDecimal("cost_usd"),
                rs.getTimestamp("created_at").toInstant()
        );
    }
}
