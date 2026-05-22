package com.recon.agent.config;

import com.recon.agent.service.ReconInvestigateAgent;
import com.recon.agent.tools.*;
import com.recon.common.config.AppConfig;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.rag.content.retriever.ContentRetriever;
import dev.langchain4j.service.AiServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class LangChainAgentConfig {

    private static final Logger log = LoggerFactory.getLogger(LangChainAgentConfig.class);

    @Bean
    @ConditionalOnProperty(name = "recon.agent.mock-llm", havingValue = "false", matchIfMissing = true)
    public ChatLanguageModel anthropicChatModel(
            @Value("${ANTHROPIC_API_KEY}") String apiKey,
            AppConfig config) {
        return AnthropicChatModel.builder()
                .apiKey(apiKey)
                .modelName(config.agent().modelInvest())
                .maxTokens(1024)
                .build();
    }

    @Bean
    @ConditionalOnProperty(name = "recon.agent.mock-llm", havingValue = "true")
    public ChatLanguageModel mockChatModel() {
        log.warn("MOCK_LLM=true — agent will ESCALATE every case without calling Anthropic");
        return messages -> Response.from(
                AiMessage.from("[MOCK] Escalating: mock LLM enabled. Set MOCK_LLM=false and provide a real ANTHROPIC_API_KEY to investigate.")
        );
    }

    @Bean
    public ReconInvestigateAgent reconInvestigateAgent(
            ChatLanguageModel chatLanguageModel,
            SearchPgTransactionsTool searchTool,
            GetChargebackStatusTool chargebackTool,
            GetSettlementHistoryTool settlementTool,
            ComputeFeeBreakdownTool feeTool,
            ProposeResolutionTool proposeTool,
            ContentRetriever contentRetriever) {
        return AiServices.builder(ReconInvestigateAgent.class)
                .chatLanguageModel(chatLanguageModel)
                .tools(searchTool, chargebackTool, settlementTool, feeTool, proposeTool)
                .contentRetriever(contentRetriever)
                .build();
    }
}
