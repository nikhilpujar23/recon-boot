package com.recon.agent.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.ledger.repo.SettlementLineRepository;
import com.recon.pii.RedactionGateway;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class GetSettlementHistoryTool {

    private final SettlementLineRepository settlementRepo;
    private final RedactionGateway         redactionGateway;
    private final ObjectMapper             json;

    public GetSettlementHistoryTool(SettlementLineRepository settlementRepo,
                                    RedactionGateway redactionGateway,
                                    ObjectMapper json) {
        this.settlementRepo  = settlementRepo;
        this.redactionGateway = redactionGateway;
        this.json            = json;
    }

    @Tool("Retrieve all settlement file lines for a given RRN across all ingested files")
    public String getSettlementHistory(
            @P("Reference Retrieval Number to look up") String rrn) {

        return redactionGateway.execute("get_settlement_history", "rrn=" + rrn, () -> {
            List<Map<String, Object>> rows = settlementRepo.findByRrn(rrn)
                    .stream()
                    .map(l -> Map.<String, Object>of(
                            "file_id",      l.fileId(),
                            "line_no",      l.lineNo(),
                            "amount_paise", l.amountPaise(),
                            "fee_paise",    l.feePaise(),
                            "net_paise",    l.netPaise(),
                            "status",       l.status(),
                            "ingested_at",  l.ingestedAt().toString()
                    ))
                    .toList();
            try {
                return json.writeValueAsString(rows);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
