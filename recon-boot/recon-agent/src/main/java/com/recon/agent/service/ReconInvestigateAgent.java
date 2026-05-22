package com.recon.agent.service;

import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.service.UserMessage;

public interface ReconInvestigateAgent {

    @SystemMessage("""
            You are a payment reconciliation specialist. Investigate unmatched or partially \
            matched transactions between an internal ledger and a bank settlement file.

            For each case:
            1. Use the available tools to gather evidence.
            2. Analyse the data carefully.
            3. Call proposeResolution EXACTLY ONCE with your conclusion.

            Resolution types:
            - PROPOSED  : confident resolution (confidence >= 0.7)
            - ESCALATE  : insufficient evidence or conflicting data

            Never guess. Escalate when uncertain.
            """)
    String investigate(@UserMessage String casePrompt);
}
