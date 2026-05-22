package com.recon.ledger.repo;

import com.recon.common.model.PgTransaction;
import com.recon.common.model.TxnStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class PgTransactionRepository {

    private final JdbcClient jdbc;

    public PgTransactionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<PgTransaction> findByRrn(String rrn) {
        return jdbc.sql("SELECT * FROM pg_transactions WHERE rrn = :rrn")
                .param("rrn", rrn)
                .query(this::map)
                .optional();
    }

    public Optional<PgTransaction> findByUtr(String utr) {
        return jdbc.sql("SELECT * FROM pg_transactions WHERE utr = :utr")
                .param("utr", utr)
                .query(this::map)
                .optional();
    }

    public List<PgTransaction> findByRrnAndAmountRange(String rrn, long minPaise, long maxPaise) {
        return jdbc.sql("""
                SELECT * FROM pg_transactions
                WHERE rrn = :rrn AND amount_paise BETWEEN :min AND :max
                """)
                .param("rrn", rrn)
                .param("min", minPaise)
                .param("max", maxPaise)
                .query(this::map)
                .list();
    }

    public List<PgTransaction> search(String rrn, String utr, Long minPaise, Long maxPaise,
                                      Instant dateFrom, Instant dateTo) {
        var sql = new StringBuilder("SELECT * FROM pg_transactions WHERE 1=1");
        var q = jdbc.sql(""); // placeholder; we build dynamically below

        // Simple dynamic query via native JdbcClient chaining
        String finalSql = "SELECT * FROM pg_transactions WHERE 1=1"
                + (rrn != null ? " AND rrn = '" + rrn.replace("'", "''") + "'" : "")
                + (utr != null ? " AND utr = '" + utr.replace("'", "''") + "'" : "")
                + (minPaise != null ? " AND amount_paise >= " + minPaise : "")
                + (maxPaise != null ? " AND amount_paise <= " + maxPaise : "")
                + (dateFrom != null ? " AND created_at >= '" + dateFrom + "'" : "")
                + (dateTo != null ? " AND created_at <= '" + dateTo + "'" : "")
                + " LIMIT 50";

        return jdbc.sql(finalSql).query(this::map).list();
    }

    private PgTransaction map(ResultSet rs, int rowNum) throws SQLException {
        return new PgTransaction(
                rs.getLong("id"),
                rs.getString("txn_id"),
                rs.getString("rrn"),
                rs.getString("utr"),
                rs.getString("payer_vpa"),
                rs.getString("payee_vpa"),
                rs.getLong("amount_paise"),
                TxnStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant()
        );
    }
}
