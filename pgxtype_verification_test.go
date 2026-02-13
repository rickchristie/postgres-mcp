//go:build integration

// This file verifies that convertValue (called inside Query pipeline) correctly
// converts all PostgreSQL types to JSON-friendly Go types. Each test:
//  1. Creates a table via setupTable (DDL+DML)
//  2. Queries via p.Query() which internally calls convertValue
//  3. Asserts exact output values using reflect.DeepEqual
//
// Requires pgflock running (port 9776).

package pgmcp_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

// pgxTypeConfig returns a config with DDL and CREATE EXTENSION enabled.
func pgxTypeConfig() pgmcp.Config {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Protection.AllowCreateExtension = true
	return config
}

// assertColumn asserts the values of a single column across all rows.
func assertColumn(t *testing.T, rows []map[string]interface{}, col string, expected []interface{}) {
	t.Helper()
	if len(rows) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows))
	}
	for i, exp := range expected {
		got := rows[i][col]
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("row %d col %q: expected %v (%T), got %v (%T)", i, col, exp, exp, got, got)
		}
	}
}

// queryRows is a helper that runs a SELECT and returns the rows from QueryOutput.
func queryRows(t *testing.T, p *pgmcp.PostgresMcp, sql string) []map[string]interface{} {
	t.Helper()
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: sql})
	if output.Error != "" {
		t.Fatalf("query failed: %s", output.Error)
	}
	return output.Rows
}

// ---------------------------------------------------------------------------
// Integer Types
// ---------------------------------------------------------------------------

func TestPgxTypes_SmallInt(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v smallint)`)
	setupTable(t, p, `INSERT INTO t VALUES (0),(1),(-1),(32767),(-32768),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		int16(0), int16(1), int16(-1), int16(32767), int16(-32768), nil,
	})
}

func TestPgxTypes_Integer(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v integer)`)
	setupTable(t, p, `INSERT INTO t VALUES (0),(1),(-1),(2147483647),(-2147483648),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		int32(0), int32(1), int32(-1), int32(2147483647), int32(-2147483648), nil,
	})
}

func TestPgxTypes_BigInt(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v bigint)`)
	setupTable(t, p, `INSERT INTO t VALUES (0),(1),(-1),(9007199254740993),(9223372036854775807),(-9223372036854775808),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		int64(0), int64(1), int64(-1), int64(9007199254740993), int64(9223372036854775807), int64(-9223372036854775808), nil,
	})
}

func TestPgxTypes_Serial(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (id serial PRIMARY KEY, name text)`)
	setupTable(t, p, `INSERT INTO t (name) VALUES ('a'),('b')`)
	rows := queryRows(t, p, `SELECT id, name FROM t ORDER BY id`)
	assertColumn(t, rows, "id", []interface{}{int32(1), int32(2)})
	assertColumn(t, rows, "name", []interface{}{"a", "b"})
}

func TestPgxTypes_BigSerial(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (id bigserial PRIMARY KEY, name text)`)
	setupTable(t, p, `INSERT INTO t (name) VALUES ('a'),('b')`)
	rows := queryRows(t, p, `SELECT id, name FROM t ORDER BY id`)
	assertColumn(t, rows, "id", []interface{}{int64(1), int64(2)})
	assertColumn(t, rows, "name", []interface{}{"a", "b"})
}

// ---------------------------------------------------------------------------
// Numeric / Decimal Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Numeric(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v numeric(20,5))`)
	setupTable(t, p, `INSERT INTO t VALUES (12345.67890),(0),(-99999.99999),(0.00001),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// pgtype.Numeric -> string via MarshalJSON
	assertColumn(t, rows, "v", []interface{}{
		"12345.67890", "0.00000", "-99999.99999", "0.00001", nil,
	})
}

func TestPgxTypes_NumericNoPrecision(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v numeric)`)
	setupTable(t, p, `INSERT INTO t VALUES (123456789012345678901234567890),(0.000000000000000001),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"123456789012345678901234567890", "0.000000000000000001", nil,
	})
}

func TestPgxTypes_NumericSpecial(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v numeric)`)
	setupTable(t, p, `INSERT INTO t VALUES ('NaN'::numeric),('Infinity'::numeric),('-Infinity'::numeric),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"NaN", "Infinity", "-Infinity", nil,
	})
}

func TestPgxTypes_Real(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v real)`)
	setupTable(t, p, `INSERT INTO t VALUES (0),(1.5),(-1.5),('NaN'::real),('Infinity'::real),('-Infinity'::real),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		float32(0), float32(1.5), float32(-1.5), "NaN", "Infinity", "-Infinity", nil,
	})
}

func TestPgxTypes_DoublePrecision(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v double precision)`)
	setupTable(t, p, `INSERT INTO t VALUES (0),(1.5),(-1.5),('NaN'::float8),('Infinity'::float8),('-Infinity'::float8),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		float64(0), float64(1.5), float64(-1.5), "NaN", "Infinity", "-Infinity", nil,
	})
}

// ---------------------------------------------------------------------------
// Monetary Type
// ---------------------------------------------------------------------------

func TestPgxTypes_Money(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v money)`)
	setupTable(t, p, `INSERT INTO t VALUES ('$0.00'),('$1,234.56'),('-$99.99'),('$999,999,999.99'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// money returns as string
	assertColumn(t, rows, "v", []interface{}{
		"$0.00", "$1,234.56", "-$99.99", "$999,999,999.99", nil,
	})
}

// ---------------------------------------------------------------------------
// Character Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Char(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v char(10))`)
	setupTable(t, p, `INSERT INTO t VALUES ('hello'),(''),('ñ'),('日本語'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// char(10) right-pads with spaces
	assertColumn(t, rows, "v", []interface{}{
		"hello     ", "          ", "ñ         ", "日本語       ", nil,
	})
}

func TestPgxTypes_Varchar(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v varchar(255))`)
	setupTable(t, p, `INSERT INTO t VALUES ('hello'),(''),('ñoño'),('日本語テスト'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"hello", "", "ñoño", "日本語テスト", nil,
	})
}

func TestPgxTypes_Text(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v text)`)
	longStr := strings.Repeat("a", 10000)
	setupTable(t, p, fmt.Sprintf(`INSERT INTO t VALUES ('hello'),(''),('multi
line
text'),('special chars: \t \\ ''quote'''),('%s'),(NULL)`, longStr))
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"hello", "", "multi\nline\ntext", "special chars: \t \\ 'quote'", longStr, nil,
	})
}

// ---------------------------------------------------------------------------
// Binary Type
// ---------------------------------------------------------------------------

func TestPgxTypes_ByteA(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v bytea)`)
	setupTable(t, p, `INSERT INTO t VALUES (decode('deadbeef', 'hex')),(decode('', 'hex')),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// bytea -> base64 encoded string
	assertColumn(t, rows, "v", []interface{}{
		base64.StdEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef}),
		"", // empty bytea -> base64 of empty bytes = ""
		nil,
	})
}

func TestPgxTypes_ByteA_JsonLookalike(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v bytea)`)
	// Bytea containing bytes that start with '{' - should NOT be parsed as JSON
	setupTable(t, p, `INSERT INTO t VALUES ('{"not":"json"}'::bytea)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// The raw bytes of the string '{"not":"json"}' get base64-encoded
	expected := base64.StdEncoding.EncodeToString([]byte(`{"not":"json"}`))
	assertColumn(t, rows, "v", []interface{}{expected})
}

// ---------------------------------------------------------------------------
// Date/Time Types
// ---------------------------------------------------------------------------

func TestPgxTypes_TimestampTZ(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v timestamptz)`)
	setupTable(t, p, `INSERT INTO t VALUES
		('2024-01-15 10:30:00+05:30'),
		('1970-01-01 00:00:00+00'),
		('2099-12-31 23:59:59.999999+00'),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// timestamptz -> time.Time -> RFC3339Nano string
	// Exact string depends on server timezone, so verify format with regex
	rfc3339Re := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)
	for i := 0; i < 3; i++ {
		v, ok := rows[i]["v"].(string)
		if !ok {
			t.Errorf("row %d: expected string, got %T", i, rows[i]["v"])
			continue
		}
		if !rfc3339Re.MatchString(v) {
			t.Errorf("row %d: expected RFC3339Nano format, got %q", i, v)
		}
	}
	if rows[3]["v"] != nil {
		t.Errorf("row 3: expected nil, got %v (%T)", rows[3]["v"], rows[3]["v"])
	}
}

func TestPgxTypes_Timestamp(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v timestamp)`)
	setupTable(t, p, `INSERT INTO t VALUES
		('2024-01-15 10:30:00'),
		('1970-01-01 00:00:00'),
		('2099-12-31 23:59:59.999999'),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// timestamp without timezone -> time.Time -> RFC3339Nano
	rfc3339Re := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)
	for i := 0; i < 3; i++ {
		v, ok := rows[i]["v"].(string)
		if !ok {
			t.Errorf("row %d: expected string, got %T", i, rows[i]["v"])
			continue
		}
		if !rfc3339Re.MatchString(v) {
			t.Errorf("row %d: expected RFC3339Nano format, got %q", i, v)
		}
	}
	if rows[3]["v"] != nil {
		t.Errorf("row 3: expected nil, got %v (%T)", rows[3]["v"], rows[3]["v"])
	}
}

func TestPgxTypes_Date(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v date)`)
	setupTable(t, p, `INSERT INTO t VALUES ('2024-01-15'),('1970-01-01'),('9999-12-31'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// date -> time.Time -> RFC3339Nano, time component zeroed
	dateRe := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T00:00:00`)
	for i := 0; i < 3; i++ {
		v, ok := rows[i]["v"].(string)
		if !ok {
			t.Errorf("row %d: expected string, got %T", i, rows[i]["v"])
			continue
		}
		if !dateRe.MatchString(v) {
			t.Errorf("row %d: expected date RFC3339Nano format with zeroed time, got %q", i, v)
		}
	}
	if rows[3]["v"] != nil {
		t.Errorf("row 3: expected nil, got %v (%T)", rows[3]["v"], rows[3]["v"])
	}
}

func TestPgxTypes_Time(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v time)`)
	setupTable(t, p, `INSERT INTO t VALUES ('10:30:00'),('00:00:00'),('23:59:59.999999'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// pgtype.Time -> "HH:MM:SS" or "HH:MM:SS.UUUUUU"
	assertColumn(t, rows, "v", []interface{}{
		"10:30:00", "00:00:00", "23:59:59.999999", nil,
	})
}

func TestPgxTypes_TimeTZ(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v timetz)`)
	setupTable(t, p, `INSERT INTO t VALUES ('10:30:00+05:30'),('00:00:00+00'),('23:59:59.999999-07'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// timetz returns as string directly from pgx (not pgtype.Time)
	for i := 0; i < 3; i++ {
		v, ok := rows[i]["v"].(string)
		if !ok {
			t.Errorf("row %d: expected string, got %T (%v)", i, rows[i]["v"], rows[i]["v"])
		}
		if v == "" {
			t.Errorf("row %d: expected non-empty timetz string", i)
		}
	}
	if rows[3]["v"] != nil {
		t.Errorf("row 3: expected nil, got %v (%T)", rows[3]["v"], rows[3]["v"])
	}
}

func TestPgxTypes_Interval(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v interval)`)
	setupTable(t, p, `INSERT INTO t VALUES
		('1 year 2 months 3 days 4 hours 5 minutes 6 seconds'),
		('-3 days -2 hours'),
		('0 seconds'),
		('1 month -5 days 3 hours'),
		('@ 1 year 2 mons'),
		('1 day 00:00:00.000001'),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"1 year(s) 2 mon(s) 3 day(s) 4h5m6s",
		"-3 day(s) -2h0m0s",
		"0",
		"1 mon(s) -5 day(s) 3h0m0s",
		"1 year(s) 2 mon(s)",
		"1 day(s) 1\u00b5s", // time.Duration formats 1 microsecond as "1µs"
		nil,
	})
}

// ---------------------------------------------------------------------------
// Boolean Type
// ---------------------------------------------------------------------------

func TestPgxTypes_Boolean(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v boolean)`)
	setupTable(t, p, `INSERT INTO t VALUES (true),(false),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{true, false, nil})
}

// ---------------------------------------------------------------------------
// UUID Type
// ---------------------------------------------------------------------------

func TestPgxTypes_UUID(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
	setupTable(t, p, `CREATE TABLE t (v uuid)`)
	setupTable(t, p, `INSERT INTO t VALUES
		(uuid_generate_v4()),
		('00000000-0000-0000-0000-000000000000'),
		('ffffffff-ffff-ffff-ffff-ffffffffffff'),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// uuid_generate_v4() is unpredictable, assert format
	uuidRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	v0, ok := rows[0]["v"].(string)
	if !ok {
		t.Errorf("row 0: expected string, got %T", rows[0]["v"])
	} else if !uuidRe.MatchString(v0) {
		t.Errorf("row 0: expected UUID format, got %q", v0)
	}
	// Known UUIDs
	if rows[1]["v"] != "00000000-0000-0000-0000-000000000000" {
		t.Errorf("row 1: expected zero UUID, got %v", rows[1]["v"])
	}
	if rows[2]["v"] != "ffffffff-ffff-ffff-ffff-ffffffffffff" {
		t.Errorf("row 2: expected all-f UUID, got %v", rows[2]["v"])
	}
	if rows[3]["v"] != nil {
		t.Errorf("row 3: expected nil, got %v", rows[3]["v"])
	}
}

// ---------------------------------------------------------------------------
// Network Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Inet(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v inet)`)
	setupTable(t, p, `INSERT INTO t VALUES
		('192.168.1.1/24'),
		('192.168.1.100'),
		('::1/128'),
		('fe80::1/64'),
		('0.0.0.0/0'),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// netip.Prefix -> string via .String()
	assertColumn(t, rows, "v", []interface{}{
		"192.168.1.1/24",
		"192.168.1.100/32", // single host gets /32
		"::1/128",
		"fe80::1/64",
		"0.0.0.0/0",
		nil,
	})
}

func TestPgxTypes_Cidr(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v cidr)`)
	setupTable(t, p, `INSERT INTO t VALUES
		('10.0.0.0/8'),
		('192.168.0.0/16'),
		('::1/128'),
		('2001:db8::/32'),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"10.0.0.0/8",
		"192.168.0.0/16",
		"::1/128",
		"2001:db8::/32",
		nil,
	})
}

func TestPgxTypes_MacAddr(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v macaddr)`)
	setupTable(t, p, `INSERT INTO t VALUES ('08:00:2b:01:02:03'),('ff:ff:ff:ff:ff:ff'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// net.HardwareAddr -> string via .String()
	assertColumn(t, rows, "v", []interface{}{
		"08:00:2b:01:02:03", "ff:ff:ff:ff:ff:ff", nil,
	})
}

func TestPgxTypes_MacAddr8(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v macaddr8)`)
	setupTable(t, p, `INSERT INTO t VALUES ('08:00:2b:01:02:03:04:05'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"08:00:2b:01:02:03:04:05", nil,
	})
}

// ---------------------------------------------------------------------------
// JSON Types
// ---------------------------------------------------------------------------

func TestPgxTypes_JSONB_Object(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	setupTable(t, p, `INSERT INTO t VALUES ('{"name":"test","age":30}'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	// JSONB object -> map[string]interface{} with float64 for numbers
	assertColumn(t, rows, "v", []interface{}{
		map[string]interface{}{"name": "test", "age": float64(30)},
	})
}

func TestPgxTypes_JSONB_Array(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	setupTable(t, p, `INSERT INTO t VALUES ('[1,2,3]'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	assertColumn(t, rows, "v", []interface{}{
		[]interface{}{float64(1), float64(2), float64(3)},
	})
}

func TestPgxTypes_JSONB_Nested(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	setupTable(t, p, `INSERT INTO t VALUES ('{"a":{"b":{"c":[1,true,null]}}}'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	assertColumn(t, rows, "v", []interface{}{
		map[string]interface{}{
			"a": map[string]interface{}{
				"b": map[string]interface{}{
					"c": []interface{}{float64(1), true, nil},
				},
			},
		},
	})
}

func TestPgxTypes_JSONB_Null(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	setupTable(t, p, `INSERT INTO t VALUES ('null'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	// JSON null -> nil
	assertColumn(t, rows, "v", []interface{}{nil})
}

func TestPgxTypes_JSONB_ScalarString(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	setupTable(t, p, `INSERT INTO t VALUES ('"just a string"'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	assertColumn(t, rows, "v", []interface{}{"just a string"})
}

func TestPgxTypes_JSONB_ScalarNumber(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	setupTable(t, p, `INSERT INTO t VALUES ('42'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	assertColumn(t, rows, "v", []interface{}{float64(42)})
}

func TestPgxTypes_JSONB_ScalarBool(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	setupTable(t, p, `INSERT INTO t VALUES ('true'::jsonb),('false'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{true, false})
}

func TestPgxTypes_JSONB_LargeInt(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	// 2^53+1 -- known to lose precision inside pgx (pgx limitation)
	setupTable(t, p, `INSERT INTO t VALUES ('{"id":9007199254740993}'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	// pgx parses large int as float64, losing precision (known limitation)
	assertColumn(t, rows, "v", []interface{}{
		map[string]interface{}{"id": float64(9007199254740992)}, // loses 1 due to float64
	})
}

func TestPgxTypes_JSONB_Empty(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	setupTable(t, p, `INSERT INTO t VALUES ('{}'::jsonb),('[]'::jsonb)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		map[string]interface{}{},
		[]interface{}{},
	})
}

func TestPgxTypes_JSONB_ColumnNull(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v jsonb)`)
	// SQL NULL (column is null), not JSON null
	setupTable(t, p, `INSERT INTO t VALUES (NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	assertColumn(t, rows, "v", []interface{}{nil})
}

func TestPgxTypes_JSON(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v json)`)
	setupTable(t, p, `INSERT INTO t VALUES
		('{"key":"value"}'::json),
		('[1,2]'::json),
		('null'::json),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// json and jsonb return identical Go types
	assertColumn(t, rows, "v", []interface{}{
		map[string]interface{}{"key": "value"},
		[]interface{}{float64(1), float64(2)},
		nil, // JSON null
		nil, // SQL NULL
	})
}

// ---------------------------------------------------------------------------
// Array Types
// ---------------------------------------------------------------------------

func TestPgxTypes_TextArray(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v text[])`)
	setupTable(t, p, `INSERT INTO t VALUES
		(ARRAY['a','b','c']),
		(ARRAY[]::text[]),
		(ARRAY['with "quotes"','with, comma','with \\ backslash']),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		[]interface{}{"a", "b", "c"},
		[]interface{}{},
		[]interface{}{`with "quotes"`, "with, comma", `with \ backslash`},
		nil,
	})
}

func TestPgxTypes_IntArray(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v int[])`)
	setupTable(t, p, `INSERT INTO t VALUES
		(ARRAY[1,2,3]),
		(ARRAY[]::int[]),
		(ARRAY[2147483647,-2147483648]),
		(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		[]interface{}{int32(1), int32(2), int32(3)},
		[]interface{}{},
		[]interface{}{int32(2147483647), int32(-2147483648)},
		nil,
	})
}

func TestPgxTypes_BigIntArray(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v bigint[])`)
	setupTable(t, p, `INSERT INTO t VALUES (ARRAY[9007199254740993]),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		[]interface{}{int64(9007199254740993)},
		nil,
	})
}

func TestPgxTypes_BoolArray(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v boolean[])`)
	setupTable(t, p, `INSERT INTO t VALUES (ARRAY[true,false,true]),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		[]interface{}{true, false, true},
		nil,
	})
}

func TestPgxTypes_UUIDArray(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
	setupTable(t, p, `CREATE TABLE t (v uuid[])`)
	setupTable(t, p, `INSERT INTO t VALUES (ARRAY[uuid_generate_v4(),uuid_generate_v4()]),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// First row: array of 2 UUID strings (unpredictable values)
	arr, ok := rows[0]["v"].([]interface{})
	if !ok {
		t.Fatalf("row 0: expected []interface{}, got %T", rows[0]["v"])
	}
	if len(arr) != 2 {
		t.Fatalf("row 0: expected 2 elements, got %d", len(arr))
	}
	uuidRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	for i, elem := range arr {
		s, ok := elem.(string)
		if !ok {
			t.Errorf("row 0 element %d: expected string, got %T", i, elem)
		} else if !uuidRe.MatchString(s) {
			t.Errorf("row 0 element %d: expected UUID format, got %q", i, s)
		}
	}
	// Second row: NULL
	if rows[1]["v"] != nil {
		t.Errorf("row 1: expected nil, got %v", rows[1]["v"])
	}
}

func TestPgxTypes_ArrayWithNulls(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v text[])`)
	setupTable(t, p, `INSERT INTO t VALUES (ARRAY['a',NULL,'c'])`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	assertColumn(t, rows, "v", []interface{}{
		[]interface{}{"a", nil, "c"},
	})
}

func TestPgxTypes_2DArray(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v int[][])`)
	setupTable(t, p, `INSERT INTO t VALUES ('{{1,2},{3,4}}')`)
	rows := queryRows(t, p, `SELECT v FROM t`)
	// 2D arrays flattened to 1D by pgx (known pgx limitation)
	assertColumn(t, rows, "v", []interface{}{
		[]interface{}{int32(1), int32(2), int32(3), int32(4)},
	})
}

// ---------------------------------------------------------------------------
// Enum Type
// ---------------------------------------------------------------------------

func TestPgxTypes_Enum(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TYPE mood AS ENUM ('happy','sad','neutral')`)
	setupTable(t, p, `CREATE TABLE t (v mood)`)
	setupTable(t, p, `INSERT INTO t VALUES ('happy'),('sad'),('neutral'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// enum -> string
	assertColumn(t, rows, "v", []interface{}{"happy", "sad", "neutral", nil})
}

// ---------------------------------------------------------------------------
// Range Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Int4Range(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v int4range)`)
	setupTable(t, p, `INSERT INTO t VALUES ('[1,10)'),('(,)'),('[5,5]'),('empty'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// pgtype.Range[interface{}] -> formatted string
	// Note: [5,5] with integers is canonicalized by PG to [5,6)
	assertColumn(t, rows, "v", []interface{}{
		"[1,10)",
		"(,)",
		"[5,6)", // PG canonicalizes [5,5] to [5,6) for integer ranges
		"empty",
		nil,
	})
}

func TestPgxTypes_Int8Range(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v int8range)`)
	setupTable(t, p, `INSERT INTO t VALUES ('[1,9223372036854775807)'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"[1,9223372036854775807)",
		nil,
	})
}

func TestPgxTypes_NumRange(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v numrange)`)
	setupTable(t, p, `INSERT INTO t VALUES ('[1.5,10.5)'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// numrange bounds are pgtype.Numeric -> recursively converted to string
	assertColumn(t, rows, "v", []interface{}{
		"[1.5,10.5)",
		nil,
	})
}

func TestPgxTypes_TsRange(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v tsrange)`)
	setupTable(t, p, `INSERT INTO t VALUES ('[2024-01-01,2024-12-31)'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// tsrange bounds are time.Time -> RFC3339Nano strings
	v, ok := rows[0]["v"].(string)
	if !ok {
		t.Fatalf("row 0: expected string, got %T", rows[0]["v"])
	}
	if !strings.HasPrefix(v, "[") {
		t.Errorf("row 0: expected range starting with '[', got %q", v)
	}
	if !strings.HasSuffix(v, ")") {
		t.Errorf("row 0: expected range ending with ')', got %q", v)
	}
	if rows[1]["v"] != nil {
		t.Errorf("row 1: expected nil, got %v", rows[1]["v"])
	}
}

func TestPgxTypes_TsTzRange(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v tstzrange)`)
	setupTable(t, p, `INSERT INTO t VALUES ('[2024-01-01 00:00:00+00,2024-12-31 23:59:59+00)'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	v, ok := rows[0]["v"].(string)
	if !ok {
		t.Fatalf("row 0: expected string, got %T", rows[0]["v"])
	}
	if !strings.HasPrefix(v, "[") {
		t.Errorf("row 0: expected range starting with '[', got %q", v)
	}
	if !strings.HasSuffix(v, ")") {
		t.Errorf("row 0: expected range ending with ')', got %q", v)
	}
	if rows[1]["v"] != nil {
		t.Errorf("row 1: expected nil, got %v", rows[1]["v"])
	}
}

func TestPgxTypes_DateRange(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v daterange)`)
	setupTable(t, p, `INSERT INTO t VALUES ('[2024-01-01,2024-12-31)'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// daterange bounds are time.Time -> RFC3339Nano with zeroed time
	v, ok := rows[0]["v"].(string)
	if !ok {
		t.Fatalf("row 0: expected string, got %T", rows[0]["v"])
	}
	if !strings.HasPrefix(v, "[") {
		t.Errorf("row 0: expected range starting with '[', got %q", v)
	}
	if rows[1]["v"] != nil {
		t.Errorf("row 1: expected nil, got %v", rows[1]["v"])
	}
}

// ---------------------------------------------------------------------------
// Geometric Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Point(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v point)`)
	setupTable(t, p, `INSERT INTO t VALUES ('(1.5,2.5)'),('(0,0)'),('(-1.5,-2.5)'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"(1.5,2.5)", "(0,0)", "(-1.5,-2.5)", nil,
	})
}

func TestPgxTypes_Line(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v line)`)
	setupTable(t, p, `INSERT INTO t VALUES ('{1,2,3}'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{"{1,2,3}", nil})
}

func TestPgxTypes_Lseg(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v lseg)`)
	setupTable(t, p, `INSERT INTO t VALUES ('[(0,0),(1,1)]'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{"[(0,0),(1,1)]", nil})
}

func TestPgxTypes_Box(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v box)`)
	setupTable(t, p, `INSERT INTO t VALUES ('(1,1),(0,0)'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{"(1,1),(0,0)", nil})
}

func TestPgxTypes_Path(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v path)`)
	// closed path (parentheses) and open path (brackets)
	setupTable(t, p, `INSERT INTO t VALUES ('((0,0),(1,1),(2,0))'),('[(0,0),(1,1)]'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"((0,0),(1,1),(2,0))", // closed
		"[(0,0),(1,1)]",      // open
		nil,
	})
}

func TestPgxTypes_Polygon(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v polygon)`)
	setupTable(t, p, `INSERT INTO t VALUES ('((0,0),(1,0),(1,1),(0,1))'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"((0,0),(1,0),(1,1),(0,1))", nil,
	})
}

func TestPgxTypes_Circle(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v circle)`)
	setupTable(t, p, `INSERT INTO t VALUES ('<(1,1),5>'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{"<(1,1),5>", nil})
}

// ---------------------------------------------------------------------------
// Bit String Types
// ---------------------------------------------------------------------------

func TestPgxTypes_Bit(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v bit(8))`)
	setupTable(t, p, `INSERT INTO t VALUES (B'10101010'),(B'00000000'),(B'11111111'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"10101010", "00000000", "11111111", nil,
	})
}

func TestPgxTypes_VarBit(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v bit varying(16))`)
	setupTable(t, p, `INSERT INTO t VALUES (B'1'),(B'10101010'),(B'1010101010101010'),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	assertColumn(t, rows, "v", []interface{}{
		"1", "10101010", "1010101010101010", nil,
	})
}

// ---------------------------------------------------------------------------
// Text Search Types
// ---------------------------------------------------------------------------

func TestPgxTypes_TsVector(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v tsvector)`)
	setupTable(t, p, `INSERT INTO t VALUES (to_tsvector('english','the quick brown fox')),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// tsvector returns as string
	v, ok := rows[0]["v"].(string)
	if !ok {
		t.Fatalf("row 0: expected string, got %T", rows[0]["v"])
	}
	// tsvector contains stemmed words, check key words are present
	if !strings.Contains(v, "brown") || !strings.Contains(v, "fox") || !strings.Contains(v, "quick") {
		t.Errorf("row 0: expected tsvector with 'brown', 'fox', 'quick', got %q", v)
	}
	if rows[1]["v"] != nil {
		t.Errorf("row 1: expected nil, got %v", rows[1]["v"])
	}
}

func TestPgxTypes_TsQuery(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v tsquery)`)
	setupTable(t, p, `INSERT INTO t VALUES (to_tsquery('english','quick & fox')),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// tsquery returns as string
	v, ok := rows[0]["v"].(string)
	if !ok {
		t.Fatalf("row 0: expected string, got %T", rows[0]["v"])
	}
	// tsquery contains stemmed words with & operator
	if !strings.Contains(v, "quick") || !strings.Contains(v, "fox") || !strings.Contains(v, "&") {
		t.Errorf("row 0: expected tsquery with 'quick & fox', got %q", v)
	}
	if rows[1]["v"] != nil {
		t.Errorf("row 1: expected nil, got %v", rows[1]["v"])
	}
}

// ---------------------------------------------------------------------------
// XML Type
// ---------------------------------------------------------------------------

func TestPgxTypes_XML(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (v xml)`)
	setupTable(t, p, `INSERT INTO t VALUES ('<root><item>test</item></root>'::xml),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// xml returns as []uint8 same as bytea -> base64 encoded (known limitation)
	xmlBytes := []byte("<root><item>test</item></root>")
	assertColumn(t, rows, "v", []interface{}{
		base64.StdEncoding.EncodeToString(xmlBytes), nil,
	})
}

// ---------------------------------------------------------------------------
// Composite / Domain Types
// ---------------------------------------------------------------------------

func TestPgxTypes_CompositeType(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TYPE address AS (street text, city text, zip text)`)
	setupTable(t, p, `CREATE TABLE t (v address)`)
	setupTable(t, p, `INSERT INTO t VALUES (ROW('123 Main St','Springfield','62701')::address),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// composite type -> string
	assertColumn(t, rows, "v", []interface{}{
		`("123 Main St",Springfield,62701)`, nil,
	})
}

func TestPgxTypes_Domain(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)`)
	setupTable(t, p, `CREATE TABLE t (v positive_int)`)
	setupTable(t, p, `INSERT INTO t VALUES (1),(100),(NULL)`)
	rows := queryRows(t, p, `SELECT v FROM t ORDER BY ctid`)
	// domain -> underlying Go type (int32 for integer domain)
	assertColumn(t, rows, "v", []interface{}{int32(1), int32(100), nil})
}

// ---------------------------------------------------------------------------
// NULL Across All Major Types
// ---------------------------------------------------------------------------

func TestPgxTypes_AllNulls(t *testing.T) {
	t.Parallel()
	p, _ := newTestInstance(t, pgxTypeConfig())
	setupTable(t, p, `CREATE TABLE t (
		a smallint, b integer, c bigint, d numeric, e real, f double precision,
		g text, h bytea, i boolean, j timestamptz, k date, l interval,
		m uuid, n inet, o jsonb, p int[]
	)`)
	setupTable(t, p, `INSERT INTO t DEFAULT VALUES`)
	rows := queryRows(t, p, `SELECT * FROM t`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	for _, col := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"} {
		if rows[0][col] != nil {
			t.Errorf("col %q: expected nil, got %v (%T)", col, rows[0][col], rows[0][col])
		}
	}
}
