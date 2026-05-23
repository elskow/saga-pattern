package dbobservability

import (
	"testing"

	"github.com/XSAM/otelsql"
)

func TestSQLSpanNameUsesReadableOperationNames(t *testing.T) {
	tests := []struct {
		method otelsql.Method
		want   string
	}{
		{method: otelsql.MethodConnExec, want: "db.exec"},
		{method: otelsql.MethodConnQuery, want: "db.query"},
		{method: otelsql.MethodConnBeginTx, want: "db.transaction.begin"},
		{method: otelsql.MethodTxCommit, want: "db.transaction.commit"},
		{method: otelsql.MethodTxRollback, want: "db.transaction.rollback"},
	}

	for _, test := range tests {
		if got := sqlSpanName(t.Context(), test.method, ""); got != test.want {
			t.Fatalf("sqlSpanName(%s) = %q, want %q", test.method, got, test.want)
		}
	}
}

func TestSQLSpanFilterSuppressesConnectionNoise(t *testing.T) {
	for _, method := range []otelsql.Method{otelsql.MethodConnResetSession, otelsql.MethodRows, otelsql.MethodConnectorConnect, otelsql.MethodConnPing} {
		if sqlSpanFilter(t.Context(), method, "", nil) {
			t.Fatalf("sqlSpanFilter(%s) = true, want false", method)
		}
	}

	for _, method := range []otelsql.Method{otelsql.MethodConnExec, otelsql.MethodConnQuery, otelsql.MethodTxCommit} {
		if !sqlSpanFilter(t.Context(), method, "", nil) {
			t.Fatalf("sqlSpanFilter(%s) = false, want true", method)
		}
	}
}
