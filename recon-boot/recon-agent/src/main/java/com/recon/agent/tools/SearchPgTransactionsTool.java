package com.recon.agent.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recon.ledger.repo.PgTransactionRepository;
import com.recon.pii.RedactionGateway;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Component
public class SearchPgTransactionsTool {

    private final PgTransactionRepository txnRepo;
    private final RedactionGateway        redactionGateway;
    private final ObjectMapper            json;

    public SearchPgTransactionsTool(PgTransactionRepository txnRepo,
                                    RedactionGateway redactionGateway,
                                    ObjectMapper json) {
        this.txnRepo          = txnRepo;
        this.redactionGateway = redactionGateway;
        this.json             = json;
    }

    @Tool("Search internal ledger transactions by RRN, UTR, amount range, and date range")
    public String search(
            @P("RRN reference number; pass empty string if not known") String rrn,
            @P("UTR reference number; pass empty string if not known") String utr,
            @P("minimum amount in paise (0 for no lower bound)") long amountMinPaise,
            @P("maximum amount in paise (0 for no upper bound)") long amountMaxPaise,
            @P("start of date range as ISO-8601 string; pass empty string for no filter") String dateFrom,
            @P("end of date range as ISO-8601 string; pass empty string for no filter") String dateTo) {

        String  rrnParam = rrn.isBlank()      ? null : rrn;
        String  utrParam = utr.isBlank()      ? null : utr;
        Long    minPaise = amountMinPaise == 0 ? null : amountMinPaise;
        Long    maxPaise = amountMaxPaise == 0 ? null : amountMaxPaise;
        Instant from     = dateFrom.isBlank() ? null : Instant.parse(dateFrom);
        Instant to       = dateTo.isBlank()   ? null : Instant.parse(dateTo);

        return redactionGateway.execute("search_pg_transactions", "rrn=" + rrn + ",utr=" + utr, () -> {
            List<Map<String, Object>> rows = txnRepo.search(rrnParam, utrParam, minPaise, maxPaise, from, to)
                    .stream()
                    .map(t -> Map.<String, Object>of(
                            "id",           t.id(),
                            "txn_id",       t.txnId(),
                            "rrn",          t.rrn(),
                            "utr",          String.valueOf(t.utr()),
                            "amount_paise", t.amountPaise(),
                            "status",       t.status().name(),
                            "created_at",   t.createdAt().toString()
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
