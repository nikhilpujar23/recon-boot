package com.recon.agent.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.pii.RedactionGateway;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class GetRefundsTool {

    private final JdbcClient       jdbc;
    private final RedactionGateway redactionGateway;
    private final ObjectMapper     json;

    public GetRefundsTool(JdbcClient jdbc, RedactionGateway redactionGateway, ObjectMapper json) {
        this.jdbc             = jdbc;
        this.redactionGateway = redactionGateway;
        this.json             = json;
    }

    @Tool("Fetch all merchant-initiated refunds against an original transaction RRN. " +
          "Use this to explain amount mismatches where the settled amount is less than the PG transaction amount.")
    public String getRefunds(
            @P("RRN of the original transaction") String rrn) {

        return redactionGateway.execute("get_refunds", "rrn=" + rrn, () -> {
            List<Map<String, Object>> rows = jdbc.sql("""
                    SELECT refund_id, amount_paise, status, reason, initiated_at, processed_at
                    FROM refunds
                    WHERE original_rrn = :rrn
                    ORDER BY initiated_at
                    """)
                    .param("rrn", rrn)
                    .query((rs, n) -> {
                        Map<String, Object> row = new java.util.LinkedHashMap<>();
                        row.put("refund_id",     rs.getString("refund_id"));
                        row.put("amount_paise",  rs.getLong("amount_paise"));
                        row.put("status",        rs.getString("status"));
                        row.put("reason",        rs.getString("reason"));
                        row.put("initiated_at",  rs.getTimestamp("initiated_at").toInstant().toString());
                        var processed = rs.getTimestamp("processed_at");
                        row.put("processed_at",  processed != null ? processed.toInstant().toString() : null);
                        return row;
                    })
                    .list();

            try {
                return json.writeValueAsString(Map.of("rrn", rrn, "refunds", rows));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
