package com.recon.ledger.outbox;

import com.recon.common.event.ReconRequestEvent;
import com.recon.common.messaging.ProtoCodec;
import com.recon.v1.ReconRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.util.List;

/**
 * Polls the outbox table and dispatches unpublished messages as Spring ApplicationEvents.
 * Runs every 500ms; marks published_at after successful dispatch.
 * Preserves at-least-once delivery: event publish happens before commit.
 */
@Component
public class OutboxDrainer {

    private static final Logger log = LoggerFactory.getLogger(OutboxDrainer.class);
    private static final int BATCH_SIZE = 100;

    private final JdbcClient jdbc;
    private final ApplicationEventPublisher eventPublisher;

    public OutboxDrainer(JdbcClient jdbc, ApplicationEventPublisher eventPublisher) {
        this.jdbc           = jdbc;
        this.eventPublisher = eventPublisher;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void drain() {
        List<OutboxRow> rows = jdbc.sql("""
                SELECT id, topic, partition_key, payload
                FROM outbox
                WHERE published_at IS NULL
                ORDER BY id
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """)
                .param("limit", BATCH_SIZE)
                .query(this::mapRow)
                .list();

        if (rows.isEmpty()) return;

        for (OutboxRow row : rows) {
            try {
                ReconRequest req = ProtoCodec.decode(row.payload(), ReconRequest.parser());
                eventPublisher.publishEvent(
                        new ReconRequestEvent(req.getCaseUid(), req.getFileId(), req.getLineNo()));
            } catch (Exception e) {
                log.error("Failed to dispatch outbox id={}", row.id(), e);
                continue;
            }
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id = :id")
                    .param("id", row.id())
                    .update();
        }

        log.debug("Drained {} outbox messages", rows.size());
    }

    private OutboxRow mapRow(ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new OutboxRow(
                rs.getLong("id"),
                rs.getString("topic"),
                rs.getString("partition_key"),
                rs.getBytes("payload")
        );
    }

    record OutboxRow(long id, String topic, String partitionKey, byte[] payload) {}
}
