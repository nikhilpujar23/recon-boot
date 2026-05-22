package com.recon.ledger.repo;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
public class ProcessedFileRepository {

    private final JdbcClient jdbc;

    public ProcessedFileRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Returns true if the file was newly inserted (not previously seen).
     */
    @Transactional
    public boolean markSeen(String sha256, String fileId, String filename, long bytes) {
        int rows = jdbc.sql("""
                INSERT INTO processed_files (file_hash, file_id, filename, bytes, ingested_at)
                VALUES (:hash, :fileId, :filename, :bytes, :now)
                ON CONFLICT (file_hash) DO NOTHING
                """)
                .param("hash", sha256)
                .param("fileId", fileId)
                .param("filename", filename)
                .param("bytes", bytes)
                .param("now", Timestamp.from(Instant.now()))
                .update();
        return rows > 0;
    }
}
