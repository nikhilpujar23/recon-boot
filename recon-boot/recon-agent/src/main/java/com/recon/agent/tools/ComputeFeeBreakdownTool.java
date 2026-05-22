package com.recon.agent.tools;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ComputeFeeBreakdownTool {

    private static final double MDR_RATE = 0.009; // 0.9% MDR
    private static final double GST_RATE = 0.18;  // 18% GST on MDR

    @Tool("Compute MDR, GST, and net paise for a given transaction amount and type (P2P or P2M)")
    public Map<String, Object> computeFeeBreakdown(
            @P("transaction amount in paise") long amountPaise,
            @P("transaction type: P2P (no MDR) or P2M (MDR applies)") String txnType) {

        double mdrRate = "P2M".equalsIgnoreCase(txnType) ? MDR_RATE : 0.0;
        long   mdrPaise = Math.round(amountPaise * mdrRate);
        long   gstPaise = Math.round(mdrPaise * GST_RATE);
        long   netPaise = amountPaise - mdrPaise - gstPaise;

        return Map.of(
                "amount_paise", amountPaise,
                "mdr_paise",    mdrPaise,
                "gst_paise",    gstPaise,
                "net_paise",    netPaise,
                "txn_type",     txnType
        );
    }
}
