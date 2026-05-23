package dbobservability

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/url"
	"strings"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	commoncontext "saga-pattern/common/context"
)

type ObservedDB struct {
	*sql.DB
	registration interface{ Unregister() error }
}

func OpenPostgres(dataSourceName string, poolName string) (*ObservedDB, error) {
	attrs := []attribute.KeyValue{
		semconv.DBSystemPostgreSQL,
		semconv.DBClientConnectionsPoolName(poolName),
	}
	if namespace := postgresNamespace(dataSourceName); namespace != "" {
		attrs = append(attrs, semconv.DBNamespace(namespace))
	}

	db, err := otelsql.Open("postgres", dataSourceName,
		otelsql.WithAttributes(attrs...),
		otelsql.WithAttributesGetter(contextAttributes),
		otelsql.WithSpanNameFormatter(sqlSpanName),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			DisableQuery:         true,
			OmitConnResetSession: true,
			OmitConnectorConnect: true,
			OmitRows:             true,
			OmitConnPrepare:      true,
			SpanFilter:           sqlSpanFilter,
		}),
	)
	if err != nil {
		return nil, err
	}
	registration, err := otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(attrs...))
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &ObservedDB{DB: db, registration: registration}, nil
}

func sqlSpanName(_ context.Context, method otelsql.Method, _ string) string {
	switch method {
	case otelsql.MethodConnExec, otelsql.MethodStmtExec:
		return "db.exec"
	case otelsql.MethodConnQuery, otelsql.MethodStmtQuery:
		return "db.query"
	case otelsql.MethodConnBeginTx:
		return "db.transaction.begin"
	case otelsql.MethodTxCommit:
		return "db.transaction.commit"
	case otelsql.MethodTxRollback:
		return "db.transaction.rollback"
	default:
		return string(method)
	}
}

func sqlSpanFilter(_ context.Context, method otelsql.Method, _ string, _ []driver.NamedValue) bool {
	switch method {
	case otelsql.MethodConnExec,
		otelsql.MethodConnQuery,
		otelsql.MethodStmtExec,
		otelsql.MethodStmtQuery,
		otelsql.MethodConnBeginTx,
		otelsql.MethodTxCommit,
		otelsql.MethodTxRollback:
		return true
	default:
		return false
	}
}

func (db *ObservedDB) Close() error {
	if db == nil || db.DB == nil {
		return nil
	}
	if db.registration != nil {
		if err := db.registration.Unregister(); err != nil {
			_ = db.DB.Close()
			return err
		}
	}
	return db.DB.Close()
}

func contextAttributes(ctx context.Context, _ otelsql.Method, _ string, _ []driver.NamedValue) []attribute.KeyValue {
	data, ok := commoncontext.From(ctx)
	if !ok {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, 6)
	if data.OrderID != "" && data.OrderID != "unknown" {
		attrs = append(attrs, attribute.String("order.id", data.OrderID))
	}
	if data.RequestID != "" {
		attrs = append(attrs, attribute.String("request.id", data.RequestID))
	}
	if data.CorrelationID != "" {
		attrs = append(attrs, attribute.String("correlation.id", data.CorrelationID))
	}
	if data.BenchmarkRun != "" {
		attrs = append(attrs, attribute.String("benchmark.run_label", data.BenchmarkRun))
	}
	if data.BenchmarkScene != "" {
		attrs = append(attrs, attribute.String("benchmark.scenario", data.BenchmarkScene))
	}
	if data.BenchmarkPhase != "" {
		attrs = append(attrs, attribute.String("benchmark.phase", data.BenchmarkPhase))
	}
	return attrs
}

func postgresNamespace(dataSourceName string) string {
	parsed, err := url.Parse(dataSourceName)
	if err == nil && parsed.Path != "" {
		return strings.TrimPrefix(parsed.Path, "/")
	}
	for _, field := range strings.Fields(dataSourceName) {
		key, value, ok := strings.Cut(field, "=")
		if ok && key == "dbname" {
			return value
		}
	}
	return ""
}
