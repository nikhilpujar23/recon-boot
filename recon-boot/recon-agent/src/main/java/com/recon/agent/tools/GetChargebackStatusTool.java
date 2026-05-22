package com.recon.agent.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.pii.RedactionGateway;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Component
public class GetChargebackStatusTool {

    private final JdbcClient      jdbc;
    private final RedactionGateway redactionGateway;
    private final ObjectMapper     json;

    public GetChargebackStatusTool(JdbcClient jdbc,
                                   RedactionGateway redactionGateway,
                                   ObjectMapper json) {
        this.jdbc             = jdbc;
        this.redactionGateway = redactionGateway;
        this.json             = json;
    }

    @Tool("Check if a chargeback has been filed for a given RRN. Returns chargeback amount, status, and processed date if found.")
    public String getChargebackStatus(
            @P("Reference Retrieval Number of the transaction") String rrn) {

        return redactionGateway.execute("get_chargeback_status", "rrn=" + rrn, () -> {
            Map<String, Object> result;
            try {
                Optional<Map<String, Object>> event = jdbc.sql("""
                        SELECT amount_paise, status, processed_at
                        FROM chargeback_events
                        WHERE rrn = :rrn
                        ORDER BY processed_at DESC
                        LIMIT 1
                        """)
                        .param("rrn", rrn)
                        .query((rs, n) -> Map.<String, Object>of(
                                "chargeback",   true,
                                "amount_paise", rs.getLong("amount_paise"),
                                "status",       rs.getString("status"),
                                "processed_at", rs.getTimestamp("processed_at").toInstant().toString()
                        ))
                        .optional();
                result = event.orElse(Map.of("chargeback", false, "rrn", rrn));
            } catch (Exception e) {
                // chargeback_events table may not exist in this deployment yet
                result = Map.of("chargeback", false, "rrn", rrn,
                                "note", "chargeback_events table unavailable");
            }
            try {
                return json.writeValueAsString(result);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
