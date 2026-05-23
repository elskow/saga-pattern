package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func Run(ctx context.Context, db *sql.DB, scope string, relativeDir string) error {
	if db == nil {
		return fmt.Errorf("postgres db is required")
	}
	if scope == "" {
		return fmt.Errorf("migration scope is required")
	}
	if relativeDir == "" {
		return fmt.Errorf("migration directory is required")
	}

	dir, err := resolveDir(relativeDir)
	if err != nil {
		return err
	}

	files, err := migrationFiles(dir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no sql migrations found for %s in %s", scope, dir)
	}

	lockKey := advisoryLockKey(scope)
	if _, err := db.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return fmt.Errorf("lock migrations for %s: %w", scope, err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey)
	}()

	if err := ensureTable(ctx, db); err != nil {
		return err
	}

	applied, err := appliedMigrations(ctx, db, scope)
	if err != nil {
		return err
	}

	for _, file := range files {
		if _, ok := applied[file]; ok {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			return fmt.Errorf("read migration %s/%s: %w", scope, file, err)
		}
		if err := applyFile(ctx, db, scope, file, string(contents)); err != nil {
			return err
		}
	}

	return nil
}

func resolveDir(relativeDir string) (string, error) {
	candidates := make([]string, 0, 4)
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, relativeDir))
	}
	if exePath, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exePath)
		candidates = append(candidates,
			filepath.Join(exeDir, relativeDir),
			filepath.Join(exeDir, "..", relativeDir),
			filepath.Join(exeDir, "..", "..", relativeDir),
		)
	}

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("migration directory %q not found", relativeDir)
}

func migrationFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migration directory %s: %w", dir, err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	return files, nil
}

func ensureTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS schema_migrations (
	    scope VARCHAR(255) NOT NULL,
	    name VARCHAR(255) NOT NULL,
	    applied_at TIMESTAMP NOT NULL,
	    PRIMARY KEY (scope, name)
	)`)
	if err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	return nil
}

func appliedMigrations(ctx context.Context, db *sql.DB, scope string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM schema_migrations WHERE scope = $1`, scope)
	if err != nil {
		return nil, fmt.Errorf("load applied migrations for %s: %w", scope, err)
	}
	defer rows.Close()

	applied := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan applied migration for %s: %w", scope, err)
		}
		applied[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations for %s: %w", scope, err)
	}
	return applied, nil
}

func applyFile(ctx context.Context, db *sql.DB, scope string, name string, sqlText string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %s/%s: %w", scope, name, err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, sqlText); err != nil {
		return fmt.Errorf("apply migration %s/%s: %w", scope, name, err)
	}
	if _, err := tx.ExecContext(ctx, `
	INSERT INTO schema_migrations (scope, name, applied_at)
	VALUES ($1, $2, $3)
	ON CONFLICT (scope, name) DO NOTHING`, scope, name, time.Now().UTC()); err != nil {
		return fmt.Errorf("record migration %s/%s: %w", scope, name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s/%s: %w", scope, name, err)
	}
	return nil
}

func advisoryLockKey(scope string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(scope))
	return int64(hash.Sum64())
}
