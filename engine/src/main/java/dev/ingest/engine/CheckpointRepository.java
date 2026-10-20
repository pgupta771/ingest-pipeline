package dev.ingest.engine;

import dev.ingest.core.Cursor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Manages durable extraction checkpoints and sync run state in PostgreSQL.
 */
@Repository
public class CheckpointRepository {

    private final JdbcTemplate jdbcTemplate;

    public CheckpointRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<Cursor> findCheckpoint(String stream) {
        List<String> values = jdbcTemplate.query(
                "SELECT cursor_value FROM extract_checkpoint WHERE stream = ?",
                (rs, rowNum) -> rs.getString("cursor_value"),
                stream);
        return values.stream().findFirst().map(Cursor::of);
    }

    public void saveCheckpoint(String stream, Cursor cursor, UUID runId) {
        jdbcTemplate.update(
                """
                INSERT INTO extract_checkpoint (stream, cursor_value, updated_at, run_id)
                VALUES (?, ?, NOW(), ?)
                ON CONFLICT (stream) DO UPDATE SET
                    cursor_value = EXCLUDED.cursor_value,
                    updated_at   = NOW(),
                    run_id       = EXCLUDED.run_id
                """,
                stream,
                cursor.value(),
                runId);
    }

    public void deleteCheckpoint(String stream) {
        jdbcTemplate.update("DELETE FROM extract_checkpoint WHERE stream = ?", stream);
    }

    public UUID createSyncRun(String connector, String stream, SyncMode mode) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update(
                """
                INSERT INTO sync_runs (id, connector, stream, mode, status, started_at)
                VALUES (?, ?, ?, ?, 'RUNNING', NOW())
                """,
                id,
                connector,
                stream,
                mode.name());
        return id;
    }

    public void incrementExtracted(UUID runId, int delta) {
        jdbcTemplate.update(
                "UPDATE sync_runs SET records_extracted = records_extracted + ? WHERE id = ?",
                delta,
                runId);
    }

    public void markSyncCompleted(UUID runId) {
        jdbcTemplate.update(
                "UPDATE sync_runs SET status = 'COMPLETED', finished_at = NOW() WHERE id = ?",
                runId);
    }

    public void markSyncInterrupted(UUID runId) {
        jdbcTemplate.update(
                "UPDATE sync_runs SET status = 'INTERRUPTED', finished_at = NOW() WHERE id = ?",
                runId);
    }

    public void markSyncFailed(UUID runId, String errorMessage) {
        jdbcTemplate.update(
                "UPDATE sync_runs SET status = 'FAILED', finished_at = NOW(), error_message = ? WHERE id = ?",
                errorMessage,
                runId);
    }

    public Optional<SyncRunInfo> findSyncRun(UUID runId) {
        List<SyncRunInfo> list = jdbcTemplate.query(
                "SELECT * FROM sync_runs WHERE id = ?",
                this::mapSyncRun,
                runId);
        return list.stream().findFirst();
    }

    private SyncRunInfo mapSyncRun(ResultSet rs, int rowNum) throws SQLException {
        Timestamp finished = rs.getTimestamp("finished_at");
        return new SyncRunInfo(
                rs.getObject("id", UUID.class),
                rs.getString("connector"),
                rs.getString("stream"),
                rs.getString("mode"),
                rs.getString("status"),
                rs.getTimestamp("started_at").toInstant(),
                finished != null ? finished.toInstant() : null,
                rs.getLong("records_extracted"),
                rs.getLong("records_loaded"),
                rs.getLong("records_dead"),
                rs.getString("error_message"));
    }
}
