package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"saga-pattern/common/migrations"
)

const (
	DefaultOrderDatabaseURL                 = "postgres://postgres:postgres@localhost:5432/order_db?sslmode=disable"
	DefaultPaymentDatabaseURL               = "postgres://postgres:postgres@localhost:5433/payment_db?sslmode=disable"
	DefaultInventoryDatabaseURL             = "postgres://postgres:postgres@localhost:5434/inventory_db?sslmode=disable"
	DefaultShippingDatabaseURL              = "postgres://postgres:postgres@localhost:5435/shipping_db?sslmode=disable"
	DefaultChoreographyOrderDatabaseURL     = "postgres://postgres:postgres@localhost:5436/order_db?sslmode=disable"
	DefaultChoreographyPaymentDatabaseURL   = "postgres://postgres:postgres@localhost:5437/payment_db?sslmode=disable"
	DefaultChoreographyInventoryDatabaseURL = "postgres://postgres:postgres@localhost:5438/inventory_db?sslmode=disable"
	DefaultChoreographyShippingDatabaseURL  = "postgres://postgres:postgres@localhost:5439/shipping_db?sslmode=disable"
)

type Migration struct {
	Scope string
	Dir   string
}

var nonSchemaChars = regexp.MustCompile(`[^a-z0-9_]+`)

func OpenPostgres(t *testing.T, baseURL string, schemaPrefix string, migrationSet ...Migration) *sql.DB {
	t.Helper()

	if strings.TrimSpace(baseURL) == "" {
		baseURL = strings.TrimSpace(os.Getenv("ORCHESTRATION_TEST_DATABASE_URL"))
	}
	if strings.TrimSpace(baseURL) == "" {
		t.Fatalf("postgres test database URL is required")
	}

	baseDB, err := sql.Open("postgres", baseURL)
	if err != nil {
		t.Fatalf("open postgres test database: %v", err)
	}
	if err := baseDB.PingContext(context.Background()); err != nil {
		_ = baseDB.Close()
		t.Skipf("postgres test database unavailable at %s: %v", safeDatabaseURL(baseURL), err)
	}

	schemaName := uniqueSchemaName(schemaPrefix)
	if _, err := baseDB.ExecContext(context.Background(), fmt.Sprintf(`CREATE SCHEMA %s`, schemaName)); err != nil {
		_ = baseDB.Close()
		t.Fatalf("create test schema %s: %v", schemaName, err)
	}

	schemaURL, err := withSearchPath(baseURL, schemaName)
	if err != nil {
		_ = dropSchema(baseDB, schemaName)
		_ = baseDB.Close()
		t.Fatalf("configure schema search_path: %v", err)
	}
	db, err := sql.Open("postgres", schemaURL)
	if err != nil {
		_ = dropSchema(baseDB, schemaName)
		_ = baseDB.Close()
		t.Fatalf("open schema-scoped postgres database: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		_ = dropSchema(baseDB, schemaName)
		_ = baseDB.Close()
		t.Fatalf("ping schema-scoped postgres database: %v", err)
	}

	for _, migration := range migrationSet {
		if err := migrations.Run(context.Background(), db, migration.Scope, migrationPath(migration.Dir)); err != nil {
			_ = db.Close()
			_ = dropSchema(baseDB, schemaName)
			_ = baseDB.Close()
			t.Fatalf("run migrations for %s: %v", migration.Scope, err)
		}
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = dropSchema(baseDB, schemaName)
		_ = baseDB.Close()
	})
	return db
}

func uniqueSchemaName(prefix string) string {
	clean := strings.ToLower(strings.TrimSpace(prefix))
	clean = nonSchemaChars.ReplaceAllString(clean, "_")
	clean = strings.Trim(clean, "_")
	if clean == "" {
		clean = "repo_test"
	}
	return fmt.Sprintf("%s_%d", clean, time.Now().UTC().UnixNano())
}

func withSearchPath(baseURL string, schemaName string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse database url: %w", err)
	}
	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func migrationPath(relative string) string {
	if filepath.IsAbs(relative) {
		cwd, err := os.Getwd()
		if err != nil {
			return relative
		}
		if converted, err := filepath.Rel(cwd, relative); err == nil {
			return converted
		}
		return relative
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return relative
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	abs := filepath.Join(repoRoot, relative)
	cwd, err := os.Getwd()
	if err != nil {
		return abs
	}
	if converted, err := filepath.Rel(cwd, abs); err == nil {
		return converted
	}
	return abs
}

func dropSchema(db *sql.DB, schemaName string) error {
	_, err := db.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName))
	return err
}

func safeDatabaseURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if parsed.User != nil {
		username := parsed.User.Username()
		if username != "" {
			parsed.User = url.User(username)
		}
	}
	return parsed.String()
}
