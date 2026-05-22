package com.recon.rules;

import com.recon.common.model.PgTransaction;
import com.recon.ledger.repo.PgTransactionRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * In-memory stub for PgTransactionRepository.
 * Configured per-test via the with*() setters so tests don't need Mockito.
 */
class StubPgTransactionRepository extends PgTransactionRepository {

    private Optional<PgTransaction> byRrn  = Optional.empty();
    private Optional<PgTransaction> byUtr  = Optional.empty();
    private List<PgTransaction>     range  = List.of();

    StubPgTransactionRepository() {
        super(null); // JdbcClient unused — all methods are overridden
    }

    void withByRrn(PgTransaction t)           { this.byRrn  = Optional.of(t);  }
    void withByRrn(Optional<PgTransaction> t) { this.byRrn  = t;               }
    void withByUtr(Optional<PgTransaction> t) { this.byUtr  = t;               }
    void withRange(List<PgTransaction> list)  { this.range  = list;            }

    @Override public Optional<PgTransaction> findByRrn(String rrn) { return byRrn; }
    @Override public Optional<PgTransaction> findByUtr(String utr) { return byUtr; }

    @Override
    public List<PgTransaction> findByRrnAndAmountRange(String rrn, long min, long max) {
        return range;
    }

    @Override
    public List<PgTransaction> search(String rrn, String utr, Long minPaise, Long maxPaise,
                                      Instant dateFrom, Instant dateTo) {
        return byRrn.map(List::of).orElse(List.of());
    }
}
