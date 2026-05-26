package com.recon.agent.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class GetMerchantFeeConfigTool {

    private final JdbcClient   jdbc;
    private final ObjectMapper json;

    public GetMerchantFeeConfigTool(JdbcClient jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Tool("Look up the active MDR fee slab for the merchant on a PG transaction and compute the " +
          "expected fee deduction and net settlement amount. Use this when the settlement amount " +
          "is slightly less than the PG transaction amount to check if the gap is explained by fees.")
    public String getMerchantFeeConfig(
            @P("PG transaction ID from the case context") long pgTransactionId,
            @P("PG transaction amount in paise to compute fee breakdown") long amountPaise) {

        // Resolve mcc and txn_type from pg_transactions — no VPA involved
        Optional<String[]> txnMetaOpt = jdbc.sql(
                "SELECT mcc, txn_type FROM pg_transactions WHERE id = :id")
                .param("id", pgTransactionId)
                .query((rs, n) -> new String[]{ rs.getString("mcc"), rs.getString("txn_type") })
                .optional();

        if (txnMetaOpt.isEmpty()) {
            try {
                return json.writeValueAsString(Map.of(
                        "found", false,
                        "reason", "pg_transaction not found: " + pgTransactionId));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        String mcc     = txnMetaOpt.get()[0];
        String txnType = txnMetaOpt.get()[1];

        if (mcc == null || txnType == null) {
            try {
                return json.writeValueAsString(Map.of("found", false,
                        "reason", "mcc or txn_type not set on this transaction"));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        Optional<Map<String, Object>> configOpt = jdbc.sql("""
                SELECT mdr_rate, gst_applicable, effective_from, effective_to
                FROM merchant_fee_config
                WHERE mcc            = :mcc
                  AND txn_type       = :txnType
                  AND effective_from <= CURRENT_DATE
                  AND (effective_to IS NULL OR effective_to >= CURRENT_DATE)
                ORDER BY effective_from DESC
                LIMIT 1
                """)
                .param("mcc",     mcc)
                .param("txnType", txnType)
                .query((rs, n) -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("mcc",            mcc);
                    row.put("mdr_rate",       rs.getBigDecimal("mdr_rate"));
                    row.put("gst_applicable", rs.getBoolean("gst_applicable"));
                    row.put("effective_from", rs.getDate("effective_from").toString());
                    var to = rs.getDate("effective_to");
                    row.put("effective_to",   to != null ? to.toString() : null);
                    return row;
                })
                .optional();

        if (configOpt.isEmpty()) {
            try {
                return json.writeValueAsString(Map.of("found", false, "mcc", mcc, "txn_type", txnType));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>(configOpt.get());
        result.put("found", true);
        result.put("txn_type", txnType);

        BigDecimal mdrRate    = (BigDecimal) result.get("mdr_rate");
        boolean    gstApply   = (boolean)    result.get("gst_applicable");
        long       mdrPaise   = mdrRate.multiply(BigDecimal.valueOf(amountPaise))
                                       .setScale(0, RoundingMode.HALF_UP).longValue();
        long       gstPaise   = gstApply ? Math.round(mdrPaise * 0.18) : 0L;
        long       netPaise   = amountPaise - mdrPaise - gstPaise;

        result.put("breakdown", Map.of(
                "amount_paise", amountPaise,
                "mdr_paise",    mdrPaise,
                "gst_paise",    gstPaise,
                "net_paise",    netPaise));

        try {
            return json.writeValueAsString(result);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
