package com.recon.eval;

import com.recon.common.model.PgTransaction;
import com.recon.ledger.repo.PgTransactionRepository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * In-memory stub initialized from a list of PgTransactions.
 * Used by the eval harness to avoid hitting a real database.
 */
class EvalPgTransactionRepository extends PgTransactionRepository {

    private final List<PgTransaction> data;

    EvalPgTransactionRepository(Collection<PgTransaction> data) {
        super(null); // JdbcClient unused — all relevant methods are overridden
        this.data = List.copyOf(data);
    }

    @Override
    public Optional<PgTransaction> findByRrn(String rrn) {
        return data.stream().filter(t -> rrn != null && rrn.equals(t.rrn())).findFirst();
    }

    @Override
    public Optional<PgTransaction> findByUtr(String utr) {
        return data.stream().filter(t -> utr != null && utr.equals(t.utr())).findFirst();
    }

    @Override
    public List<PgTransaction> findByRrnAndAmountRange(String rrn, long min, long max) {
        return data.stream()
                .filter(t -> rrn != null && rrn.equals(t.rrn())
                          && t.amountPaise() >= min && t.amountPaise() <= max)
                .toList();
    }

    @Override
    public List<PgTransaction> search(String rrn, String utr, Long minPaise, Long maxPaise,
                                      Instant dateFrom, Instant dateTo) {
        return data.stream()
                .filter(t -> (rrn == null || rrn.equals(t.rrn()))
                          && (utr == null || utr.equals(t.utr())))
                .toList();
    }
}
