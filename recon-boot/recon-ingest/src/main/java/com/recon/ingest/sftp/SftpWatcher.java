package com.recon.ingest.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.recon.common.messaging.ProtoCodec;
import com.recon.common.util.CaseUid;
import com.recon.ingest.parser.UdirParser;
import com.recon.ledger.repo.ProcessedFileRepository;
import com.recon.ledger.repo.SettlementLineRepository;
import com.recon.common.model.SettlementLine;
import com.recon.v1.ReconRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Vector;

/**
 * Polls SFTP drop directory every 30s.
 * Deduplicates by SHA-256 in processed_files.
 * Parses UDIR file and writes settlement_lines + outbox in one transaction.
 */
@Component
@Profile("worker")
public class SftpWatcher {

    private static final Logger log = LoggerFactory.getLogger(SftpWatcher.class);

    @Value("${recon.sftp.host:localhost}")
    private String host;

    @Value("${recon.sftp.port:2222}")
    private int port;

    @Value("${recon.sftp.user:recon}")
    private String user;

    @Value("${recon.sftp.password:recon}")
    private String password;

    @Value("${recon.sftp.remote-dir:/upload}")
    private String remoteDir;

    private final UdirParser parser;
    private final ProcessedFileRepository processedFiles;
    private final SettlementLineRepository settlementLines;

    public SftpWatcher(UdirParser parser, ProcessedFileRepository processedFiles,
                       SettlementLineRepository settlementLines) {
        this.parser = parser;
        this.processedFiles = processedFiles;
        this.settlementLines = settlementLines;
    }

    @Scheduled(fixedDelay = 5_000)
    public void poll() {
        Session session = null;
        ChannelSftp channel = null;
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(user, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect(10_000);

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();

            channel.cd(remoteDir);

            @SuppressWarnings("unchecked")
            Vector<ChannelSftp.LsEntry> entries = channel.ls(".");

            for (ChannelSftp.LsEntry entry : entries) {
                String filename = entry.getFilename();
                if (filename.startsWith(".") || !filename.endsWith(".txt")) continue;

                processFile(channel, filename);
            }
        } catch (Exception e) {
            log.error("SFTP poll failed", e);
        } finally {
            if (channel != null) channel.disconnect();
            if (session != null) session.disconnect();
        }
    }

    private void processFile(ChannelSftp channel, String filename) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        channel.get(filename, baos);
        byte[] content = baos.toByteArray();

        String sha256 = sha256Hex(content);
        String fileId = "FILE_" + sha256.substring(0, 12).toUpperCase();

        boolean isNew = processedFiles.markSeen(sha256, fileId, filename, content.length);
        if (!isNew) {
            log.debug("Skipping already-processed file: {}", filename);
            return;
        }

        log.info("Processing new file: {} sha256={}", filename, sha256);

        UdirParser.ParseResult result = parser.parse(fileId, new ByteArrayInputStream(content));
        if (!result.errors().isEmpty()) {
            log.warn("Parse errors in {}: {}", filename, result.errors());
        }

        for (SettlementLine line : result.lines()) {
            String caseUid = CaseUid.of(fileId, line.lineNo());
            ReconRequest req = ReconRequest.newBuilder()
                    .setFileId(fileId)
                    .setLineNo(line.lineNo())
                    .setCaseUid(caseUid)
                    .build();
            settlementLines.insertIgnoreDuplicate(line, ProtoCodec.encode(req), caseUid);
        }

        log.info("Ingested {} lines from {}", result.lines().size(), filename);
    }

    private String sha256Hex(byte[] data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(md.digest(data));
    }
}
