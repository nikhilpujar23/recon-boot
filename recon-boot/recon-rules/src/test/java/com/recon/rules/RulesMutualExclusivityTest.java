package com.recon.rules;

import com.recon.common.model.*;
import com.recon.common.config.AppConfig;
import com.recon.rules.engine.Rule;
import com.recon.rules.rules.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RulesMutualExclusivityTest {

    private StubPgTransactionRepository txnRepo;
    private AppConfig config;

    @BeforeEach
    void setUp() {
        txnRepo = new StubPgTransactionRepository();
        config  = new AppConfig(
                new AppConfig.Agent("haiku", "sonnet", 6, 30, true),
                new AppConfig.Rules(1),
                new AppConfig.Pii("key", "key"),
                new AppConfig.Api("token", 60),
                new AppConfig.Retry(1.0, 3, 5.0, 25.0)
        );
    }

    private SettlementLine line(String rrn, String utr, long amount) {
        return new SettlementLine(1L, "FILE_TEST", 1, rrn, utr, amount, 0L, amount, "SUCCESS", null, Instant.now());
    }

    private PgTransaction txn(Long id, String rrn, String utr, long amount, TxnStatus status) {
        return new PgTransaction(id, "TXN_" + id, rrn, utr, "payer@vpa", "payee@vpa",
                amount, status, Instant.now(), Instant.now());
    }

    @Test
    void exactRrnRuleMatchesOnRrnAmountAndSuccessStatus() {
        txnRepo.withByRrn(txn(1L, "RRN001", "UTR001", 10000L, TxnStatus.SUCCESS));

        Rule rule = new ExactRrnRule(txnRepo);
        RuleMatch m = rule.evaluate(line("RRN001", "UTR001", 10000L), new HashSet<>());

        assertTrue(m.matched());
        assertEquals(MatchType.EXACT, m.matchType());
        assertEquals(0, m.confidence().compareTo(java.math.BigDecimal.ONE));
    }

    @Test
    void exactRrnRuleDoesNotMatchOnFailedStatus() {
        txnRepo.withByRrn(txn(1L, "RRN001", "UTR001", 10000L, TxnStatus.FAILED));

        Rule rule = new ExactRrnRule(txnRepo);
        RuleMatch m = rule.evaluate(line("RRN001", "UTR001", 10000L), new HashSet<>());

        assertFalse(m.matched());
    }

    @Test
    void duplicateRuleFiresWhenRrnSeenBefore() {
        Rule rule = new DuplicateRule();
        Set<String> seen = new HashSet<>(List.of("RRN001"));
        RuleMatch m = rule.evaluate(line("RRN001", "UTR001", 10000L), seen);

        assertTrue(m.matched());
        assertEquals(MatchType.DUPLICATE, m.matchType());
    }

    @Test
    void duplicateRuleDoesNotFireForNewRrn() {
        Rule rule = new DuplicateRule();
        RuleMatch m = rule.evaluate(line("RRN_NEW", "UTR001", 10000L), new HashSet<>());
        assertFalse(m.matched());
    }

    @Test
    void toleranceRuleMatchesWithinOnePaise() {
        txnRepo.withRange(List.of(txn(1L, "RRN001", "UTR001", 10001L, TxnStatus.SUCCESS)));

        Rule rule = new ToleranceRule(txnRepo, config);
        RuleMatch m = rule.evaluate(line("RRN001", "UTR001", 10000L), new HashSet<>());

        assertTrue(m.matched());
        assertEquals(MatchType.TOLERANCE, m.matchType());
    }

    @Test
    void amountMismatchRuleFiresWhenRrnMatchesButAmountDiffers() {
        txnRepo.withByRrn(txn(1L, "RRN001", "UTR001", 99999L, TxnStatus.SUCCESS));

        Rule rule = new AmountMismatchRule(txnRepo);
        RuleMatch m = rule.evaluate(line("RRN001", "UTR001", 10000L), new HashSet<>());

        assertTrue(m.matched());
        assertEquals(MatchType.AMOUNT_MISMATCH, m.matchType());
    }

    @Test
    void missingLegRuleFiresWhenNoRrnOrUtrFound() {
        txnRepo.withByRrn(Optional.empty());
        txnRepo.withByUtr(Optional.empty());

        Rule rule = new MissingLegRule(txnRepo);
        RuleMatch m = rule.evaluate(line("RRN_GHOST", "UTR_GHOST", 5000L), new HashSet<>());

        assertTrue(m.matched());
        assertEquals(MatchType.MISSING_LEG, m.matchType());
    }

    @Test
    void rulesAreIdempotent() {
        txnRepo.withByRrn(txn(1L, "RRN001", "UTR001", 10000L, TxnStatus.SUCCESS));

        Rule rule = new ExactRrnRule(txnRepo);
        SettlementLine l = line("RRN001", "UTR001", 10000L);

        RuleMatch m1 = rule.evaluate(l, new HashSet<>());
        RuleMatch m2 = rule.evaluate(l, new HashSet<>());
        assertEquals(m1.matchType(),  m2.matchType());
        assertEquals(m1.confidence(), m2.confidence());
    }
}
