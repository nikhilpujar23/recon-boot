package com.recon.rag;

import com.recon.common.config.AppConfig;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.rag.content.Content;
import dev.langchain4j.rag.content.retriever.ContentRetriever;
import dev.langchain4j.rag.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@ConditionalOnProperty(name = "recon.rag.enabled", havingValue = "true", matchIfMissing = false)
public class ReconContentRetriever implements ContentRetriever {

    private static final Logger log = LoggerFactory.getLogger(ReconContentRetriever.class);
    private static final Pattern UUID_PATTERN =
            Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    private final JdbcClient jdbc;
    private final EmbeddingModel embeddingModel;
    private final AppConfig config;

    public ReconContentRetriever(JdbcClient jdbc, EmbeddingModel embeddingModel, AppConfig config) {
        this.jdbc = jdbc;
        this.embeddingModel = embeddingModel;
        this.config = config;
    }

    @Override
    public List<Content> retrieve(Query query) {
        AppConfig.Rag rag = config.rag();
        if (!rag.enabled()) return List.of();

        String text = query.text();
        Embedding queryEmbedding = embeddingModel.embed(text).content();
        String vectorLiteral = VectorUtils.toLiteral(queryEmbedding.vector());
        double distanceThreshold = 1.0 - rag.minScore();

        Optional<String> matchTypeFilter = rag.filterByMatchType()
                ? extractMatchType(text)
                : Optional.empty();

        List<Content> results = matchTypeFilter
                .map(mt -> queryWithMatchType(vectorLiteral, distanceThreshold, mt, rag.topK()))
                .orElseGet(() -> queryAll(vectorLiteral, distanceThreshold, rag.topK()));

        log.debug("rag.retrieve query_uid={} returned={}", extractCaseUid(text).orElse("?"), results.size());
        return results;
    }

    private List<Content> queryWithMatchType(String vec, double dist, String matchType, int topK) {
        return jdbc.sql("""
                SELECT content, 1 - (embedding <=> CAST(:vec AS vector)) AS score
                FROM case_embeddings
                WHERE match_type = :matchType
                  AND embedding <=> CAST(:vec AS vector) < :dist
                ORDER BY embedding <=> CAST(:vec AS vector)
                LIMIT :topK
                """)
                .param("vec", vec)
                .param("matchType", matchType)
                .param("dist", dist)
                .param("topK", topK)
                .query(this::mapContent)
                .list();
    }

    private List<Content> queryAll(String vec, double dist, int topK) {
        return jdbc.sql("""
                SELECT content, 1 - (embedding <=> CAST(:vec AS vector)) AS score
                FROM case_embeddings
                WHERE embedding <=> CAST(:vec AS vector) < :dist
                ORDER BY embedding <=> CAST(:vec AS vector)
                LIMIT :topK
                """)
                .param("vec", vec)
                .param("dist", dist)
                .param("topK", topK)
                .query(this::mapContent)
                .list();
    }

    private Content mapContent(ResultSet rs, int row) throws SQLException {
        return Content.from(TextSegment.from(rs.getString("content")));
    }

    private Optional<String> extractCaseUid(String text) {
        Matcher m = UUID_PATTERN.matcher(text);
        if (!m.find()) return Optional.empty();
        String uid = m.group();
        return jdbc.sql("""
                SELECT match_type FROM recon_cases WHERE case_uid = :uid LIMIT 1
                """)
                .param("uid", UUID.fromString(uid))
                .query(String.class)
                .optional();
    }

    private Optional<String> extractMatchType(String text) {
        return extractCaseUid(text);
    }
}
