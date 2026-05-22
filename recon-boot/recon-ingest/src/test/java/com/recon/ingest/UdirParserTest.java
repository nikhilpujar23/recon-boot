package com.recon.ingest;

import com.recon.common.model.SettlementLine;
import com.recon.ingest.parser.UdirParser;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UdirParserTest {

    private final UdirParser parser = new UdirParser();

    private ByteArrayInputStream toStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void parsesValidUdirFile() throws Exception {
        String content = """
                HDR|FILE_TEST|2024-01-15|3
                RRN001|UTR001|10000|50|9950|SUCCESS
                RRN002|UTR002|20000|100|19900|SUCCESS
                RRN003|UTR003|5000|25|4975|FAILED
                TRAILER|3
                """;

        UdirParser.ParseResult result = parser.parse("FILE_TEST", toStream(content));

        assertEquals(3, result.lines().size(), "Should parse 3 data rows");
        assertTrue(result.errors().isEmpty(), "Should have no parse errors");
        assertEquals("FILE_TEST", result.headerFileId());
    }

    @Test
    void parsesCorrectAmounts() throws Exception {
        String content = "RRN001|UTR001|10000|50|9950|SUCCESS\n";
        UdirParser.ParseResult result = parser.parse("FILE_A", toStream(content));

        SettlementLine line = result.lines().get(0);
        assertEquals(10000L, line.amountPaise());
        assertEquals(50L, line.feePaise());
        assertEquals(9950L, line.netPaise());
        assertEquals("RRN001", line.rrn());
        assertEquals("UTR001", line.utr());
    }

    @Test
    void assignsSequentialLineNumbers() throws Exception {
        String content = """
                RRN001|UTR001|1000|0|1000|SUCCESS
                RRN002|UTR002|2000|0|2000|SUCCESS
                """;
        UdirParser.ParseResult result = parser.parse("FILE_B", toStream(content));

        assertEquals(1, result.lines().get(0).lineNo());
        assertEquals(2, result.lines().get(1).lineNo());
    }

    @Test
    void toleratesEmptyLines() throws Exception {
        String content = "\nRRN001|UTR001|1000|0|1000|SUCCESS\n\n";
        UdirParser.ParseResult result = parser.parse("FILE_C", toStream(content));
        assertEquals(1, result.lines().size());
    }

    @Test
    void handlesTrailerCountMismatchGracefully() throws Exception {
        // Trailer says 2 but only 1 data row — should still return the 1 row
        String content = """
                RRN001|UTR001|1000|0|1000|SUCCESS
                TRAILER|2
                """;
        UdirParser.ParseResult result = parser.parse("FILE_D", toStream(content));
        assertEquals(1, result.lines().size(), "Should still return the rows that were parsed");
    }

    @Test
    void emptyFileReturnsNoLines() throws Exception {
        UdirParser.ParseResult result = parser.parse("FILE_EMPTY", toStream(""));
        assertTrue(result.lines().isEmpty());
        assertTrue(result.errors().isEmpty());
    }

    @Test
    void fileIdIsSetOnEachLine() throws Exception {
        String content = "RRN001|UTR001|1000|0|1000|SUCCESS\n";
        UdirParser.ParseResult result = parser.parse("FILE_XYZ", toStream(content));
        assertEquals("FILE_XYZ", result.lines().get(0).fileId());
    }
}
