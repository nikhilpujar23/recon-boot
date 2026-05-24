package com.recon.agent.outbox;

import com.recon.common.event.ReconInvestigateEvent;
import com.recon.common.messaging.ProtoCodec;
import com.recon.v1.ReconInvestigate;
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
 * Polls the outbox for 'recon.investigate' rows written by rules-worker and dispatches
 * them as local Spring events so ReconInvestigateListener can invoke the LLM agent.
 * Bridges the rules-worker → agent-worker JVM boundary via the shared DB outbox.
 */
@Component
public class InvestigateOutboxDrainer {

    private static final Logger log = LoggerFactory.getLogger(InvestigateOutboxDrainer.class);
    // 1 at a time: prevents thundering herd against Groq's 12k TPM free tier
    private static final int BATCH_SIZE = 1;

    private final JdbcClient jdbc;
    private final ApplicationEventPublisher eventPublisher;

    public InvestigateOutboxDrainer(JdbcClient jdbc, ApplicationEventPublisher eventPublisher) {
        this.jdbc           = jdbc;
        this.eventPublisher = eventPublisher;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void drain() {
        List<OutboxRow> rows = jdbc.sql("""
                SELECT id, payload
                FROM outbox
                WHERE topic = 'recon.investigate' AND published_at IS NULL
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
                ReconInvestigate msg = ProtoCodec.decode(row.payload(), ReconInvestigate.parser());
                eventPublisher.publishEvent(new ReconInvestigateEvent(msg.getCaseUid()));
            } catch (Exception e) {
                log.error("Failed to dispatch investigate outbox id={}", row.id(), e);
                continue;
            }
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id = :id")
                    .param("id", row.id())
                    .update();
        }

        log.debug("Drained {} investigate outbox messages", rows.size());
    }

    private OutboxRow mapRow(ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new OutboxRow(rs.getLong("id"), rs.getBytes("payload"));
    }

    record OutboxRow(long id, byte[] payload) {}
}
