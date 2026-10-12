package francis

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeActorStore generates a WAL large enough that a checkpoint has to decide what to do with it.
func writeActorStore(t *testing.T, db *sql.DB, rows int) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE state (id TEXT PRIMARY KEY, payload BLOB)`)
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(`INSERT INTO state (id, payload) VALUES (?, randomblob(8192))`)
	require.NoError(t, err)
	for index := range rows {
		_, execErr := stmt.Exec(fmt.Sprintf("key-%05d", index))
		require.NoError(t, execErr)
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())
}

func checkpointAndMeasure(t *testing.T, dsn, storePath string, rows int) int64 {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	require.NoError(t, db.Ping())
	writeActorStore(t, db, rows)

	var busy, logFrames, checkpointed int
	require.NoError(t, db.QueryRow("PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &logFrames, &checkpointed))
	require.Zero(t, busy, "no other connection holds the store, so the checkpoint must complete")

	// journal_size_limit is applied when the next transaction commits after the log resets, which is
	// exactly what the actor store does on every dispatch pass.
	_, err = db.Exec(`INSERT INTO state (id, payload) VALUES ('post-checkpoint', randomblob(1024))`)
	require.NoError(t, err)

	info, err := os.Stat(storePath + "-wal")
	require.NoError(t, err)
	return info.Size()
}

// TestStoreURLCapsWALSize proves the journal_size_limit pragma reaches the actor store: the same
// writes leave a multi-megabyte WAL behind without it, and a checkpoint truncates the log to the
// configured ceiling with it.
func TestStoreURLCapsWALSize(t *testing.T) {
	const rows = 900

	withLimit := filepath.Join(t.TempDir(), "arcane.db")
	withLimitURL, err := StoreURL("file:" + withLimit + "?_pragma=journal_mode(WAL)&_txlock=immediate")
	require.NoError(t, err)
	withLimitSize := checkpointAndMeasure(t, withLimitURL, filepath.Join(filepath.Dir(withLimit), "arcane.francis.db"), rows)

	withoutLimit := filepath.Join(t.TempDir(), "arcane.db")
	withoutLimitURL := "file:" + filepath.Join(filepath.Dir(withoutLimit), "arcane.francis.db") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(2500)&_pragma=synchronous(NORMAL)"
	withoutLimitSize := checkpointAndMeasure(t, withoutLimitURL, filepath.Join(filepath.Dir(withoutLimit), "arcane.francis.db"), rows)

	require.LessOrEqual(t, withLimitSize, int64(actorWALSizeLimit), "the limit must cap the WAL after a checkpoint")
	require.Greater(t, withoutLimitSize, int64(actorWALSizeLimit), "without the pragma SQLite keeps the grown WAL for reuse")
}

// TestStoreURLKeepsExplicitWALLimit guards the override: an operator who sets journal_size_limit on
// DATABASE_URL keeps it instead of the default.
func TestStoreURLKeepsExplicitWALLimit(t *testing.T) {
	storeURL, err := StoreURL("file:" + filepath.Join(t.TempDir(), "arcane.db") + "?_pragma=journal_size_limit(1048576)")
	require.NoError(t, err)
	require.Contains(t, storeURL, "_pragma=journal_size_limit%281048576%29")
	require.NotContains(t, storeURL, "_pragma=journal_size_limit%284194304%29")
}

// TestStoreURLRaisesBusyTimeout guards the actor store's lock-wait floor: the inherited 2500 ms gave
// up four times in a four-second window during a concurrent scan burst, so the store opens with at
// least ten seconds while an explicitly larger value is preserved.
func TestStoreURLRaisesBusyTimeout(t *testing.T) {
	for input, want := range map[string]string{
		"file:/data/arcane.db":                            "busy_timeout%2810000%29",
		"file:/data/arcane.db?_pragma=busy_timeout(2500)": "busy_timeout%2810000%29",
	} {
		storeURL, err := StoreURL(input)
		require.NoError(t, err)
		require.Contains(t, storeURL, want)
	}

	kept, err := StoreURL("file:/data/arcane.db?_pragma=busy_timeout(15000)")
	require.NoError(t, err)
	require.Contains(t, kept, "busy_timeout%2815000%29")
	require.NotContains(t, kept, "busy_timeout%2810000%29")
}

// TestStoreURLLockWaitOutlivesTheArcaneDefault proves the raised floor end to end: a writer that
// meets a three-second lock still succeeds, where the inherited 2500 ms would have returned
// SQLITE_BUSY.
func TestStoreURLLockWaitOutlivesTheArcaneDefault(t *testing.T) {
	mainPath := filepath.Join(t.TempDir(), "arcane.db")
	storeURL, err := StoreURL("file:" + mainPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(2500)&_txlock=immediate")
	require.NoError(t, err)

	holder, err := sql.Open("sqlite", storeURL)
	require.NoError(t, err)
	defer func() { require.NoError(t, holder.Close()) }()
	require.NoError(t, holder.Ping())
	_, err = holder.Exec(`CREATE TABLE t (id INTEGER)`)
	require.NoError(t, err)

	writer, err := sql.Open("sqlite", storeURL)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	require.NoError(t, writer.Ping())

	tx, err := holder.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO t VALUES (1)`)
	require.NoError(t, err)

	done := make(chan error, 1)
	started := time.Now()
	go func() {
		_, execErr := writer.Exec(`INSERT INTO t VALUES (2)`)
		done <- execErr
	}()
	time.Sleep(3 * time.Second)
	require.NoError(t, tx.Commit())
	require.NoError(t, <-done, "the actor store must wait out the inherited 2500 ms instead of failing")
	require.GreaterOrEqual(t, time.Since(started), 2900*time.Millisecond)
}
