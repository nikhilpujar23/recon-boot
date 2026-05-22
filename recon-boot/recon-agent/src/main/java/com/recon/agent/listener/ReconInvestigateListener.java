package com.recon.agent.listener;

import com.recon.agent.orchestrator.AgentOrchestrator;
import com.recon.common.event.ReconInvestigateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class ReconInvestigateListener {

    private static final Logger log = LoggerFactory.getLogger(ReconInvestigateListener.class);

    private final AgentOrchestrator orchestrator;

    public ReconInvestigateListener(AgentOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Async
    @EventListener
    public void onReconInvestigate(ReconInvestigateEvent event) {
        try {
            log.info("Received investigate event case_uid={}", event.caseUid());
            orchestrator.investigate(event.caseUid());
        } catch (Exception e) {
            log.error("Failed to process investigate event case_uid={}", event.caseUid(), e);
            throw e;
        }
    }
}
