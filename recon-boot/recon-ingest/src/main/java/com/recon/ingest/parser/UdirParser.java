package com.recon.ingest.parser;

import com.recon.common.model.SettlementLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses UDIR pipe-delimited settlement files.
 * Header: version|file_id|date|total_records
 * Data rows: line_no|rrn|utr|amount|fee|net|status
 * Footer: TRAILER|count
 */
@Component
public class UdirParser {

    private static final Logger log = LoggerFactory.getLogger(UdirParser.class);
    private static final String DELIMITER = "|";

    public ParseResult parse(String fileId, InputStream inputStream) throws Exception {
        List<SettlementLine> lines = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        String headerFileId = null;
        int trailerCount = -1;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String raw;
            int rowNum = 0;

            while ((raw = reader.readLine()) != null) {
                raw = raw.trim();
                if (raw.isEmpty()) continue;

                String[] parts = raw.split("\\|", -1);
                String recordType = parts[0].trim().toUpperCase();

                switch (recordType) {
                    case "HDR", "HEADER" -> {
                        if (parts.length >= 2) headerFileId = parts[1].trim();
                    }
                    case "TRL", "TRAILER" -> {
                        if (parts.length >= 2) {
                            try { trailerCount = Integer.parseInt(parts[1].trim()); }
                            catch (NumberFormatException ignored) {}
                        }
                    }
                    default -> {
                        rowNum++;
                        try {
                            SettlementLine line = parseDataRow(fileId, rowNum, parts, raw);
                            lines.add(line);
                        } catch (Exception e) {
                            errors.add("Row %d: %s".formatted(rowNum, e.getMessage()));
                            log.warn("Parse error row={} file={}: {}", rowNum, fileId, e.getMessage());
                        }
                    }
                }
            }
        }

        if (trailerCount >= 0 && trailerCount != lines.size()) {
            log.warn("Trailer count mismatch: expected={} actual={} file={}", trailerCount, lines.size(), fileId);
        }

        return new ParseResult(lines, errors, headerFileId);
    }

    private SettlementLine parseDataRow(String fileId, int lineNo, String[] parts, String raw) {
        // Expected columns: rrn|utr|amount_paise|fee_paise|net_paise|status
        // Be lenient: map by index, default missing fields
        String rrn        = parts.length > 0 ? parts[0].trim() : "";
        String utr        = parts.length > 1 ? parts[1].trim() : "";
        long amountPaise  = parts.length > 2 ? parseLong(parts[2]) : 0L;
        long feePaise     = parts.length > 3 ? parseLong(parts[3]) : 0L;
        long netPaise     = parts.length > 4 ? parseLong(parts[4]) : amountPaise - feePaise;
        String status     = parts.length > 5 ? parts[5].trim() : "UNKNOWN";

        Map<String, Object> rawMap = new HashMap<>();
        rawMap.put("raw_line", raw);

        return new SettlementLine(null, fileId, lineNo, rrn, utr,
                amountPaise, feePaise, netPaise, status, rawMap, Instant.now());
    }

    private long parseLong(String s) {
        try {
            return Long.parseLong(s.trim().replace(",", "").replace(".", ""));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    public record ParseResult(List<SettlementLine> lines, List<String> errors, String headerFileId) {}
}
