//go:build integration

// TEMPORARY: These are raw pgx type discovery tests — they query pgx directly
// and log the Go types returned, without running values through convertValue.
// They must be replaced with tests that pipe values through convertValue and
// assert on the converted output. After replacement, the 3 currently-failing
// tests (NumericSpecial, Real, DoublePrecision) will pass since convertValue
// handles NaN/Inf before json.Marshal sees them.
//
// This file verifies the actual Go types returned by pgx rows.Values() when
// using QueryExecModeExec (simple protocol). Results inform convertValue
// implementation. Requires pgflock running (port 9776).
//
// Each test:
//  1. Creates a table with the target column type
//  2. Inserts multiple values covering edge cases (including NULL)
//  3. Queries with QueryExecModeExec
//  4. Logs the actual Go type (%T) and value for every row
//  5. Verifies json.Marshal succeeds on every value (convertValue round-trip safety)

package pgmcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickchristie/govner/pgflock/client"
)

const (
	pgflockLockerPort = 9776
	pgflockPassword   = "pgflock"
)

func acquireTestDB(t *testing.T) string {
	t.Helper()
	connStr, err := client.Lock(pgflockLockerPort, t.Name(), pgflockPassword)
	if err != nil {
		t.Fatalf("Failed to acquire test database: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Unlock(pgflockLockerPort, pgflockPassword, connStr)
	})
	return connStr
}

func acquirePool(t *testing.T, connStr string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse pool config: %v", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// execOrFatal runs a SQL statement and fails the test on error.
func execOrFatal(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), sql)
	if err != nil {
		t.Fatalf("Exec failed: %s\nSQL: %s", err, sql)
	}
}

// queryAndLog runs a SELECT and logs type/value for every column of every row.
// Also verifies each value survives json.Marshal (round-trip safety).
func queryAndLog(t *testing.T, pool *pgxpool.Pool, query string) {
	t.Helper()
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	rowNum := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatalf("Failed to get values at row %d: %v", rowNum, err)
		}
		for i, v := range values {
			colName := rows.FieldDescriptions()[i].Name
			t.Logf("  row[%d].%s: Go type = %T, value = %v", rowNum, colName, v, v)

			// Verify json.Marshal doesn't panic or error — every value from
			// convertValue must be JSON-serializable.
			if _, err := json.Marshal(v); err != nil {
				t.Errorf("  row[%d].%s: json.Marshal failed on %T: %v", rowNum, colName, v, err)
			}
		}
		rowNum++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows iteration error: %v", err)
	}
	t.Logf("  Total rows: %d", rowNum)
}

// ---------------------------------------------------------------------------
// Integer Types
// ---------------------------------------------------------------------------

func TestPgxTypes_SmallInt(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v smallint)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (0),(1),(-1),(32767),(-32768),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Integer(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v integer)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (0),(1),(-1),(2147483647),(-2147483648),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_BigInt(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v bigint)`)
	// 9007199254740993 = 2^53+1, exceeds float64 precision
	execOrFatal(t, pool, `INSERT INTO t VALUES (0),(1),(-1),(9007199254740993),(9223372036854775807),(-9223372036854775808),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Serial(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (id serial PRIMARY KEY, name text)`)
	execOrFatal(t, pool, `INSERT INTO t (name) VALUES ('a'),('b')`)
	queryAndLog(t, pool, `SELECT id, name FROM t ORDER BY id`)
}

func TestPgxTypes_BigSerial(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (id bigserial PRIMARY KEY, name text)`)
	execOrFatal(t, pool, `INSERT INTO t (name) VALUES ('a'),('b')`)
	queryAndLog(t, pool, `SELECT id, name FROM t ORDER BY id`)
}

// ---------------------------------------------------------------------------
// Numeric / Decimal Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Numeric(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v numeric(20,5))`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (12345.67890),(0),(-99999.99999),(0.00001),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_NumericNoPrecision(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v numeric)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (123456789012345678901234567890),(0.000000000000000001),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_NumericSpecial(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v numeric)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('NaN'::numeric),('Infinity'::numeric),('-Infinity'::numeric),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Real(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v real)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (0),(1.5),(-1.5),(3.4028235e+38),(1.175494e-38),('NaN'::real),('Infinity'::real),('-Infinity'::real),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_DoublePrecision(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v double precision)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (0),(1.5),(-1.5),(1.7976931348623157e+308),(5e-324),('NaN'::float8),('Infinity'::float8),('-Infinity'::float8),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Monetary Type
// ---------------------------------------------------------------------------

func TestPgxTypes_Money(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v money)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('$0.00'),('$1234.56'),('-$99.99'),('$999999999.99'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Character Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Char(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v char(10))`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('hello'),(''),('ñ'),('日本語'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Varchar(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v varchar(255))`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('hello'),(''),('ñoño'),('日本語テスト'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Text(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v text)`)
	execOrFatal(t, pool, fmt.Sprintf(`INSERT INTO t VALUES ('hello'),(''),('multi
line
text'),('special chars: \t \\ ''quote'''),('🎉🚀'),('%s'),(NULL)`,
		// 10000 char string (use 'a' repeated, not null bytes which cause pgx 08P01)
		strings.Repeat("a", 10000),
	))
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Binary Type
// ---------------------------------------------------------------------------

func TestPgxTypes_ByteA(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v bytea)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (decode('deadbeef', 'hex')),(decode('', 'hex')),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_ByteA_JsonLookalike(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v bytea)`)
	// Bytea containing bytes that start with '{' — should NOT be parsed as JSON
	execOrFatal(t, pool, `INSERT INTO t VALUES ('{"not":"json"}'::bytea)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Date/Time Types
// ---------------------------------------------------------------------------

func TestPgxTypes_TimestampTZ(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v timestamptz)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		('2024-01-15 10:30:00+05:30'),
		('1970-01-01 00:00:00+00'),
		('2099-12-31 23:59:59.999999+00'),
		('epoch'::timestamptz),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Timestamp(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v timestamp)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		('2024-01-15 10:30:00'),
		('1970-01-01 00:00:00'),
		('2099-12-31 23:59:59.999999'),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Date(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v date)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('2024-01-15'),('1970-01-01'),('9999-12-31'),('epoch'::date),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Time(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v time)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('10:30:00'),('00:00:00'),('23:59:59.999999'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_TimeTZ(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v timetz)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('10:30:00+05:30'),('00:00:00+00'),('23:59:59.999999-07'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Interval(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v interval)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		('1 year 2 months 3 days 4 hours 5 minutes 6 seconds'),
		('-3 days -2 hours'),
		('0 seconds'),
		('1 month -5 days 3 hours'),
		('@ 1 year 2 mons'),
		('1 day 00:00:00.000001'),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Boolean Type
// ---------------------------------------------------------------------------

func TestPgxTypes_Boolean(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v boolean)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (true),(false),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// UUID Type
// ---------------------------------------------------------------------------

func TestPgxTypes_UUID(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
	execOrFatal(t, pool, `CREATE TABLE t (v uuid)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		(uuid_generate_v4()),
		('00000000-0000-0000-0000-000000000000'),
		('ffffffff-ffff-ffff-ffff-ffffffffffff'),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Network Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Inet(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v inet)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		('192.168.1.1/24'),
		('192.168.1.100'),
		('::1/128'),
		('fe80::1/64'),
		('0.0.0.0/0'),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Cidr(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v cidr)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		('10.0.0.0/8'),
		('192.168.0.0/16'),
		('::1/128'),
		('2001:db8::/32'),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_MacAddr(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v macaddr)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('08:00:2b:01:02:03'),('ff:ff:ff:ff:ff:ff'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_MacAddr8(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v macaddr8)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('08:00:2b:01:02:03:04:05'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// JSON Types
// ---------------------------------------------------------------------------

func TestPgxTypes_JSONB_Object(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('{"name":"test","age":30}'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_JSONB_Array(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('[1,2,3]'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_JSONB_Nested(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('{"a":{"b":{"c":[1,true,null]}}}'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_JSONB_Null(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('null'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_JSONB_ScalarString(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('"just a string"'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_JSONB_ScalarNumber(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('42'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_JSONB_ScalarBool(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('true'::jsonb),('false'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_JSONB_LargeInt(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	// 2^53+1 — known to lose precision inside pgx (pgx limitation)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('{"id":9007199254740993}'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_JSONB_Empty(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('{}'::jsonb),('[]'::jsonb)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_JSONB_ColumnNull(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v jsonb)`)
	// SQL NULL (column is null), not JSON null
	execOrFatal(t, pool, `INSERT INTO t VALUES (NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_JSON(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v json)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		('{"key":"value"}'::json),
		('[1,2]'::json),
		('null'::json),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Array Types
// ---------------------------------------------------------------------------

func TestPgxTypes_TextArray(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v text[])`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		(ARRAY['a','b','c']),
		(ARRAY[]::text[]),
		(ARRAY['with "quotes"','with, comma','with \\ backslash']),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_IntArray(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v int[])`)
	execOrFatal(t, pool, `INSERT INTO t VALUES
		(ARRAY[1,2,3]),
		(ARRAY[]::int[]),
		(ARRAY[2147483647,-2147483648]),
		(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_BigIntArray(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v bigint[])`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (ARRAY[9007199254740993]),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_BoolArray(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v boolean[])`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (ARRAY[true,false,true]),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_UUIDArray(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
	execOrFatal(t, pool, `CREATE TABLE t (v uuid[])`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (ARRAY[uuid_generate_v4(),uuid_generate_v4()]),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_ArrayWithNulls(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v text[])`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (ARRAY['a',NULL,'c'])`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

func TestPgxTypes_2DArray(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v int[][])`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('{{1,2},{3,4}}')`)
	queryAndLog(t, pool, `SELECT v FROM t`)
}

// ---------------------------------------------------------------------------
// Enum Type
// ---------------------------------------------------------------------------

func TestPgxTypes_Enum(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TYPE mood AS ENUM ('happy','sad','neutral')`)
	execOrFatal(t, pool, `CREATE TABLE t (v mood)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('happy'),('sad'),('neutral'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Range Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Int4Range(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v int4range)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('[1,10)'),('(,)'),('[5,5]'),('empty'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Int8Range(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v int8range)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('[1,9223372036854775807)'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_NumRange(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v numrange)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('[1.5,10.5)'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_TsRange(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v tsrange)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('[2024-01-01,2024-12-31)'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_TsTzRange(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v tstzrange)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('[2024-01-01 00:00:00+00,2024-12-31 23:59:59+00)'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_DateRange(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v daterange)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('[2024-01-01,2024-12-31)'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Geometric Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Point(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v point)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('(1.5,2.5)'),('(0,0)'),('(-1.5,-2.5)'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Line(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v line)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('{1,2,3}'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Lseg(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v lseg)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('[(0,0),(1,1)]'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Box(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v box)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('(1,1),(0,0)'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Path(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v path)`)
	// closed path (parentheses) and open path (brackets)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('((0,0),(1,1),(2,0))'),('[(0,0),(1,1)]'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Polygon(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v polygon)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('((0,0),(1,0),(1,1),(0,1))'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Circle(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v circle)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('<(1,1),5>'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Bit String Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Bit(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v bit(8))`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (B'10101010'),(B'00000000'),(B'11111111'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_VarBit(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v bit varying(16))`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (B'1'),(B'10101010'),(B'1010101010101010'),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Text Search Types
// ---------------------------------------------------------------------------

func TestPgxTypes_TsVector(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v tsvector)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (to_tsvector('english','the quick brown fox')),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_TsQuery(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v tsquery)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (to_tsquery('english','quick & fox')),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// XML Type
// ---------------------------------------------------------------------------

func TestPgxTypes_XML(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (v xml)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES ('<root><item>test</item></root>'::xml),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// Composite / Domain Types
// ---------------------------------------------------------------------------

func TestPgxTypes_CompositeType(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TYPE address AS (street text, city text, zip text)`)
	execOrFatal(t, pool, `CREATE TABLE t (v address)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (ROW('123 Main St','Springfield','62701')::address),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

func TestPgxTypes_Domain(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)`)
	execOrFatal(t, pool, `CREATE TABLE t (v positive_int)`)
	execOrFatal(t, pool, `INSERT INTO t VALUES (1),(100),(NULL)`)
	queryAndLog(t, pool, `SELECT v FROM t ORDER BY ctid`)
}

// ---------------------------------------------------------------------------
// NULL Across All Major Types
// ---------------------------------------------------------------------------

func TestPgxTypes_AllNulls(t *testing.T) {
	connStr := acquireTestDB(t)
	pool := acquirePool(t, connStr)
	execOrFatal(t, pool, `CREATE TABLE t (
		a smallint, b integer, c bigint, d numeric, e real, f double precision,
		g text, h bytea, i boolean, j timestamptz, k date, l interval,
		m uuid, n inet, o jsonb, p int[]
	)`)
	execOrFatal(t, pool, `INSERT INTO t DEFAULT VALUES`)
	queryAndLog(t, pool, `SELECT * FROM t`)
}
