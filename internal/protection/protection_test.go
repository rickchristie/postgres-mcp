package protection

import (
	"strings"
	"testing"
)

// helper: default config with all Allow* false, ReadOnly false.
func defaultConfig() Config {
	return Config{}
}

// helper: config with all Allow* true.
func allAllowedConfig() Config {
	return Config{
		AllowSet: true, AllowDrop: true, AllowTruncate: true, AllowDo: true,
		AllowCopyFrom: true, AllowCopyTo: true, AllowCreateFunction: true, AllowPrepare: true,
		AllowDeleteWithoutWhere: true, AllowUpdateWithoutWhere: true, AllowAlterSystem: true,
		AllowMerge: true, AllowGrantRevoke: true, AllowManageRoles: true,
		AllowCreateExtension: true, AllowLockTable: true, AllowListenNotify: true,
		AllowMaintenance: true, AllowDDL: true, AllowDiscard: true, AllowComment: true,
		AllowCreateTrigger: true, AllowCreateRule: true,
	}
}

func assertBlocked(t *testing.T, c *Checker, sql string, errContains string) {
	t.Helper()
	err := c.Check(sql)
	if err == nil {
		t.Fatalf("expected error containing %q for SQL %q, got nil", errContains, sql)
	}
	if !strings.Contains(err.Error(), errContains) {
		t.Fatalf("expected error containing %q, got %q", errContains, err.Error())
	}
}

func assertAllowed(t *testing.T, c *Checker, sql string) {
	t.Helper()
	err := c.Check(sql)
	if err != nil {
		t.Fatalf("expected SQL to be allowed: %q, got error: %v", sql, err)
	}
}

// --- Multi-Statement Detection ---

func TestMultiStatement_TwoSelects(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "SELECT 1; SELECT 2", "multi-statement queries are not allowed: found 2 statements")
}

func TestMultiStatement_SelectAndDrop(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "SELECT 1; DROP TABLE users", "multi-statement queries are not allowed: found 2 statements")
}

func TestMultiStatement_ThreeStatements(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "SELECT 1; SELECT 2; SELECT 3", "multi-statement queries are not allowed: found 3 statements")
}

func TestMultiStatement_CannotBeDisabled(t *testing.T) {
	t.Parallel()
	c := NewChecker(allAllowedConfig())
	assertBlocked(t, c, "SELECT 1; SELECT 2", "multi-statement queries are not allowed: found 2 statements")
}

func TestMultiStatement_SingleAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT 1")
}

func TestMultiStatement_EmptyStatements(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	err := c.Check(";")
	if err == nil {
		t.Fatal("expected error for empty statement")
	}
}

// --- DROP Protection ---

func TestDrop_Table(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DROP TABLE users", "DROP statements are not allowed")
}

func TestDrop_Index(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DROP INDEX idx_users", "DROP statements are not allowed")
}

func TestDrop_Schema(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DROP SCHEMA public", "DROP statements are not allowed")
}

func TestDrop_Database(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DROP DATABASE mydb", "DROP DATABASE is not allowed")
}

func TestDrop_CaseInsensitive(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "drop table users", "DROP statements are not allowed")
}

func TestDrop_WithComments(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "/* comment */ DROP TABLE users", "DROP statements are not allowed")
}

func TestDrop_IfExists(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DROP TABLE IF EXISTS users", "DROP statements are not allowed")
}

func TestDrop_Cascade(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DROP TABLE users CASCADE", "DROP statements are not allowed")
}

func TestDrop_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDrop: true})
	assertAllowed(t, c, "DROP TABLE users")
}

func TestDrop_DatabaseAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDrop: true})
	assertAllowed(t, c, "DROP DATABASE mydb")
}

// --- TRUNCATE Protection ---

func TestTruncate_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "TRUNCATE users", "TRUNCATE statements are not allowed")
}

func TestTruncate_Multiple(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "TRUNCATE users, orders", "TRUNCATE statements are not allowed")
}

func TestTruncate_Cascade(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "TRUNCATE users CASCADE", "TRUNCATE statements are not allowed")
}

func TestTruncate_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowTruncate: true})
	assertAllowed(t, c, "TRUNCATE users")
}

// --- SET Protection ---

func TestSet_SearchPath(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "SET search_path TO 'public'", "SET statements are not allowed: SET search_path")
}

func TestSet_WorkMem(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "SET work_mem = '256MB'", "SET statements are not allowed: SET work_mem")
}

func TestSet_ResetAll(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "RESET ALL", "RESET ALL is not allowed")
}

func TestSet_ResetSingle(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "RESET work_mem", "RESET statements are not allowed: RESET work_mem")
}

func TestSet_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowSet: true})
	assertAllowed(t, c, "SET work_mem = '256MB'")
}

func TestSet_ResetAllAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowSet: true})
	assertAllowed(t, c, "RESET ALL")
}

func TestSet_ResetSingleAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowSet: true})
	assertAllowed(t, c, "RESET work_mem")
}

// --- DO Block Protection ---

func TestDo_Simple(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DO $$ BEGIN RAISE NOTICE 'hello'; END $$", "DO $$ blocks are not allowed: DO blocks can execute arbitrary SQL bypassing protection checks")
}

func TestDo_WithDrop(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DO $$ BEGIN EXECUTE 'DROP TABLE users'; END $$", "DO $$ blocks are not allowed")
}

func TestDo_WithLanguage(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DO LANGUAGE plpgsql $$ BEGIN NULL; END $$", "DO $$ blocks are not allowed")
}

func TestDo_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDo: true})
	assertAllowed(t, c, "DO $$ BEGIN NULL; END $$")
}

// --- COPY FROM Protection ---

func TestCopyFrom_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COPY users FROM '/tmp/data.csv'", "COPY FROM is not allowed")
}

func TestCopyFrom_WithOptions(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, HEADER true)", "COPY FROM is not allowed")
}

func TestCopyFrom_Stdin(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COPY users FROM STDIN", "COPY FROM is not allowed")
}

func TestCopyFrom_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCopyFrom: true})
	assertAllowed(t, c, "COPY users FROM '/tmp/data.csv'")
}

func TestCopyFrom_AllowedDoesNotAffectCopyTo(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCopyFrom: true})
	assertBlocked(t, c, "COPY users TO STDOUT", "COPY TO is not allowed")
}

// --- COPY TO Protection ---

func TestCopyTo_Stdout(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COPY users TO STDOUT", "COPY TO is not allowed: can export/exfiltrate data from tables")
}

func TestCopyTo_File(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COPY users TO '/tmp/data.csv'", "COPY TO is not allowed")
}

func TestCopyTo_WithQuery(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COPY (SELECT * FROM users) TO STDOUT", "COPY TO is not allowed")
}

func TestCopyTo_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCopyTo: true})
	assertAllowed(t, c, "COPY users TO STDOUT")
}

func TestCopyTo_AllowedDoesNotAffectCopyFrom(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCopyTo: true})
	assertBlocked(t, c, "COPY users FROM '/tmp/data.csv'", "COPY FROM is not allowed")
}

// --- CREATE FUNCTION / CREATE PROCEDURE Protection ---

func TestCreateFunction_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE FUNCTION foo() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql", "CREATE FUNCTION is not allowed: can contain arbitrary SQL bypassing protection checks")
}

func TestCreateFunction_OrReplace(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE OR REPLACE FUNCTION foo() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql", "CREATE FUNCTION is not allowed")
}

func TestCreateFunction_WithArgs(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE FUNCTION add(a int, b int) RETURNS int AS $$ BEGIN RETURN a + b; END $$ LANGUAGE plpgsql", "CREATE FUNCTION is not allowed")
}

func TestCreateFunction_SQL(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE FUNCTION foo() RETURNS int AS 'SELECT 1' LANGUAGE sql", "CREATE FUNCTION is not allowed")
}

func TestCreateProcedure_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE PROCEDURE do_stuff() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$", "CREATE PROCEDURE is not allowed: can contain arbitrary SQL bypassing protection checks")
}

func TestCreateProcedure_OrReplace(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE OR REPLACE PROCEDURE do_stuff() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$", "CREATE PROCEDURE is not allowed")
}

func TestCreateFunction_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCreateFunction: true})
	assertAllowed(t, c, "CREATE FUNCTION foo() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql")
}

func TestCreateProcedure_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCreateFunction: true})
	assertAllowed(t, c, "CREATE PROCEDURE do_stuff() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$")
}

// --- PREPARE Protection ---

func TestPrepare_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "PREPARE stmt AS SELECT 1", "PREPARE statements are not allowed: prepared statements can be executed later bypassing protection checks")
}

func TestPrepare_WithParams(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "PREPARE stmt(int) AS SELECT * FROM users WHERE id = $1", "PREPARE statements are not allowed")
}

func TestPrepare_WithDML(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "PREPARE stmt AS DELETE FROM users", "PREPARE statements are not allowed")
}

func TestPrepare_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowPrepare: true})
	assertAllowed(t, c, "PREPARE stmt AS SELECT 1")
}

// --- EXECUTE Protection ---

func TestExecute_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXECUTE stmt", "EXECUTE statements are not allowed: can execute prepared statements that bypass protection checks")
}

func TestExecute_WithParams(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXECUTE stmt(1, 'test')", "EXECUTE statements are not allowed")
}

func TestExecute_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowPrepare: true})
	assertAllowed(t, c, "EXECUTE stmt")
}

// --- DEALLOCATE Protection ---

func TestDeallocate_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DEALLOCATE stmt", "DEALLOCATE statements are not allowed: managed under the same flag as PREPARE")
}

func TestDeallocate_All(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DEALLOCATE ALL", "DEALLOCATE statements are not allowed")
}

func TestDeallocate_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowPrepare: true})
	assertAllowed(t, c, "DEALLOCATE stmt")
}

// --- EXPLAIN ANALYZE Protection ---

func TestExplain_SelectAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN SELECT * FROM users")
}

func TestExplain_AnalyzeSelectAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN ANALYZE SELECT * FROM users")
}

func TestExplain_DropParseError(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN DROP TABLE users", "SQL parse error")
}

func TestExplain_AnalyzeDropParseError(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE DROP TABLE users", "SQL parse error")
}

func TestExplain_DeleteWithoutWhereBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN DELETE FROM users", "DELETE without WHERE clause is not allowed")
}

func TestExplain_AnalyzeDeleteWithoutWhereBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE DELETE FROM users", "DELETE without WHERE clause is not allowed")
}

func TestExplain_DeleteWithWhereAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN ANALYZE DELETE FROM users WHERE id = 1")
}

func TestExplain_AnalyzeUpdateWithoutWhereBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE UPDATE users SET active = false", "UPDATE without WHERE clause is not allowed")
}

func TestExplain_AnalyzeUpdateWithWhereAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN ANALYZE UPDATE users SET active = false WHERE id = 1")
}

func TestExplain_TruncateParseError(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE TRUNCATE users", "SQL parse error")
}

func TestExplain_DropParseErrorEvenWhenAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDrop: true})
	assertBlocked(t, c, "EXPLAIN DROP TABLE users", "SQL parse error")
}

func TestExplain_AnalyzeInsertAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN ANALYZE INSERT INTO users (name) VALUES ('test')")
}

func TestExplain_AnalyzeMergeBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name", "MERGE statements are not allowed")
}

func TestExplain_AnalyzeMergeAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMerge: true})
	assertAllowed(t, c, "EXPLAIN ANALYZE MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name")
}

func TestExplain_CTEDeleteWithoutWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d", "DELETE without WHERE clause is not allowed")
}

func TestExplain_CTEDeleteWithoutWhereNoAnalyze(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d", "DELETE without WHERE clause is not allowed")
}

// --- EXPLAIN: Plain EXPLAIN variants (no ANALYZE) ---

func TestExplain_InsertAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN INSERT INTO users (name) VALUES ('test')")
}

func TestExplain_UpdateWithoutWhereBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN UPDATE users SET active = false", "UPDATE without WHERE clause is not allowed")
}

func TestExplain_UpdateWithWhereAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN UPDATE users SET active = false WHERE id = 1")
}

func TestExplain_DeleteWithWhereAllowedNoAnalyze(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN DELETE FROM users WHERE id = 1")
}

func TestExplain_MergeBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name", "MERGE statements are not allowed")
}

func TestExplain_MergeAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMerge: true})
	assertAllowed(t, c, "EXPLAIN MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name")
}

// --- EXPLAIN: CREATE TABLE AS (DDL) ---

func TestExplain_CreateTableAsBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN CREATE TABLE summary AS SELECT COUNT(*) FROM users", "CREATE TABLE AS / CREATE MATERIALIZED VIEW is not allowed: DDL operations are blocked")
}

func TestExplain_AnalyzeCreateTableAsBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE CREATE TABLE summary AS SELECT COUNT(*) FROM users", "CREATE TABLE AS / CREATE MATERIALIZED VIEW is not allowed: DDL operations are blocked")
}

func TestExplain_CreateTableAsAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "EXPLAIN CREATE TABLE summary AS SELECT COUNT(*) FROM users")
}

func TestExplain_AnalyzeCreateTableAsAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "EXPLAIN ANALYZE CREATE TABLE summary AS SELECT COUNT(*) FROM users")
}

// --- EXPLAIN: CREATE MATERIALIZED VIEW AS (DDL) ---

func TestExplain_CreateMatViewBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) FROM users", "CREATE TABLE AS / CREATE MATERIALIZED VIEW is not allowed: DDL operations are blocked")
}

func TestExplain_AnalyzeCreateMatViewBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) FROM users", "CREATE TABLE AS / CREATE MATERIALIZED VIEW is not allowed: DDL operations are blocked")
}

func TestExplain_CreateMatViewAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "EXPLAIN CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) FROM users")
}

func TestExplain_AnalyzeCreateMatViewAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "EXPLAIN ANALYZE CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) FROM users")
}

// --- EXPLAIN: EXECUTE (AllowPrepare) ---

func TestExplain_ExecuteBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN EXECUTE stmt", "EXECUTE statements are not allowed: can execute prepared statements that bypass protection checks")
}

func TestExplain_AnalyzeExecuteBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE EXECUTE stmt", "EXECUTE statements are not allowed: can execute prepared statements that bypass protection checks")
}

func TestExplain_ExecuteAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowPrepare: true})
	assertAllowed(t, c, "EXPLAIN EXECUTE stmt")
}

func TestExplain_AnalyzeExecuteAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowPrepare: true})
	assertAllowed(t, c, "EXPLAIN ANALYZE EXECUTE stmt")
}

// --- EXPLAIN: REFRESH MATERIALIZED VIEW (AllowMaintenance) ---

func TestExplain_RefreshMatViewBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN REFRESH MATERIALIZED VIEW mv", "REFRESH MATERIALIZED VIEW is not allowed")
}

func TestExplain_AnalyzeRefreshMatViewBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW mv", "REFRESH MATERIALIZED VIEW is not allowed")
}

func TestExplain_RefreshMatViewAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMaintenance: true})
	assertAllowed(t, c, "EXPLAIN REFRESH MATERIALIZED VIEW mv")
}

func TestExplain_AnalyzeRefreshMatViewAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMaintenance: true})
	assertAllowed(t, c, "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW mv")
}

// --- EXPLAIN: DECLARE CURSOR (always allowed — no protection rule) ---

func TestExplain_DeclareCursorAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN DECLARE cur CURSOR FOR SELECT * FROM users")
}

func TestExplain_AnalyzeDeclareCursorAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN ANALYZE DECLARE cur CURSOR FOR SELECT * FROM users")
}

// --- EXPLAIN: Non-ExplainableStmt parse errors (TRUNCATE without ANALYZE) ---

func TestExplain_TruncateParseErrorNoAnalyze(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "EXPLAIN TRUNCATE users", "SQL parse error")
}

// --- DELETE/UPDATE with WHERE ---

func TestDeleteWithoutWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DELETE FROM users", "DELETE without WHERE clause is not allowed")
}

func TestDeleteWithWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "DELETE FROM users WHERE id = 1")
}

func TestDeleteWithComplexWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "DELETE FROM users WHERE id IN (SELECT id FROM banned)")
}

func TestDeleteWithExists(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "DELETE FROM users WHERE EXISTS (SELECT 1 FROM banned WHERE banned.uid = users.id)")
}

func TestDeleteWithoutWhere_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDeleteWithoutWhere: true})
	assertAllowed(t, c, "DELETE FROM users")
}

func TestUpdateWithoutWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "UPDATE users SET active = false", "UPDATE without WHERE clause is not allowed")
}

func TestUpdateWithWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "UPDATE users SET active = false WHERE id = 1")
}

func TestUpdateWithSubqueryWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "UPDATE users SET active = false WHERE id IN (SELECT id FROM active_users)")
}

func TestUpdateWithoutWhere_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowUpdateWithoutWhere: true})
	assertAllowed(t, c, "UPDATE users SET active = false")
}

// --- Read-Only Mode ---

func TestReadOnly_BlocksSetTransactionReadOnly(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true, AllowSet: true})
	assertBlocked(t, c, "SET default_transaction_read_only = off", "SET default_transaction_read_only is blocked in read-only mode: cannot change transaction read-only setting")
}

func TestReadOnly_BlocksSetTransactionReadOnly2(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true, AllowSet: true})
	assertBlocked(t, c, "SET transaction_read_only = false", "SET transaction_read_only is blocked in read-only mode: cannot change transaction read-only setting")
}

func TestReadOnly_BlocksResetAll(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true, AllowSet: true})
	assertBlocked(t, c, "RESET ALL", "RESET ALL is blocked in read-only mode: could disable read-only transaction setting")
}

func TestReadOnly_BlocksResetTransactionReadOnly(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true, AllowSet: true})
	assertBlocked(t, c, "RESET default_transaction_read_only", "RESET default_transaction_read_only is blocked in read-only mode")
}

func TestReadOnly_AllowsResetOther(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true, AllowSet: true})
	assertAllowed(t, c, "RESET work_mem")
}

func TestReadOnly_BlocksBeginReadWrite(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true})
	assertBlocked(t, c, "BEGIN READ WRITE", "BEGIN READ WRITE is blocked in read-only mode: cannot start a read-write transaction")
}

func TestReadOnly_BlocksStartTransactionReadWrite(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true})
	assertBlocked(t, c, "START TRANSACTION READ WRITE", "BEGIN READ WRITE is blocked in read-only mode: cannot start a read-write transaction")
}

func TestReadOnly_BeginReadOnlyStillBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true})
	assertBlocked(t, c, "BEGIN READ ONLY", "transaction control statements are not allowed")
}

func TestReadOnly_BeginStillBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true})
	assertBlocked(t, c, "BEGIN", "transaction control statements are not allowed")
}

func TestReadOnly_AllowsOtherSet(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true, AllowSet: true})
	assertAllowed(t, c, "SET search_path = 'public'")
}

func TestReadOnly_SetBlockedTakesPriority(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true, AllowSet: false})
	assertBlocked(t, c, "SET default_transaction_read_only = off", "SET default_transaction_read_only is blocked in read-only mode")
}

// --- ALTER SYSTEM Protection ---

func TestAlterSystem_Set(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements'", "ALTER SYSTEM is not allowed: can modify server-level configuration")
}

func TestAlterSystem_Reset(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER SYSTEM RESET shared_preload_libraries", "ALTER SYSTEM is not allowed")
}

func TestAlterSystem_ResetAll(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER SYSTEM RESET ALL", "ALTER SYSTEM is not allowed")
}

func TestAlterSystem_ArchiveCommand(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER SYSTEM SET archive_command = '/bin/malicious'", "ALTER SYSTEM is not allowed")
}

func TestAlterSystem_SSL(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER SYSTEM SET ssl = off", "ALTER SYSTEM is not allowed")
}

func TestAlterSystem_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowAlterSystem: true})
	assertAllowed(t, c, "ALTER SYSTEM SET work_mem = '256MB'")
}

// --- MERGE Protection ---

func TestMerge_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)", "MERGE statements are not allowed: MERGE can perform INSERT, UPDATE, and DELETE operations bypassing individual DML protection rules")
}

func TestMerge_WithDelete(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN DELETE", "MERGE statements are not allowed")
}

func TestMerge_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMerge: true})
	assertAllowed(t, c, "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name")
}

// --- GRANT / REVOKE Protection ---

func TestGrant_Table(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "GRANT SELECT ON users TO readonly_user", "GRANT statements are not allowed: can modify database permissions")
}

func TestGrant_AllPrivileges(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "GRANT ALL PRIVILEGES ON users TO admin_user", "GRANT statements are not allowed")
}

func TestRevoke_Table(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "REVOKE SELECT ON users FROM readonly_user", "REVOKE statements are not allowed: can modify database permissions")
}

func TestGrantRole(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "GRANT admin TO bob", "GRANT ROLE is not allowed: can modify role memberships")
}

func TestRevokeRole(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "REVOKE admin FROM bob", "REVOKE ROLE is not allowed: can modify role memberships")
}

func TestGrant_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowGrantRevoke: true})
	assertAllowed(t, c, "GRANT SELECT ON users TO readonly_user")
}

func TestRevoke_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowGrantRevoke: true})
	assertAllowed(t, c, "REVOKE SELECT ON users FROM readonly_user")
}

func TestGrantRole_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowGrantRevoke: true})
	assertAllowed(t, c, "GRANT admin TO bob")
}

// --- Role Management Protection ---

func TestCreateRole_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE ROLE testrole WITH LOGIN PASSWORD 'secret'", "CREATE ROLE/USER is not allowed: can create database roles with privileges")
}

func TestCreateUser(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE USER testuser WITH PASSWORD 'secret'", "CREATE ROLE/USER is not allowed")
}

func TestAlterRole_Superuser(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER ROLE testrole WITH SUPERUSER", "ALTER ROLE/USER is not allowed: can modify role privileges including SUPERUSER")
}

func TestAlterUser(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER USER testuser SET search_path = 'public'", "ALTER ROLE/USER is not allowed")
}

func TestDropRole(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DROP ROLE testrole", "DROP ROLE/USER is not allowed: can delete database roles")
}

func TestDropUser(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DROP USER testuser", "DROP ROLE/USER is not allowed")
}

func TestCreateRole_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowManageRoles: true})
	assertAllowed(t, c, "CREATE ROLE testrole")
}

func TestAlterRole_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowManageRoles: true})
	assertAllowed(t, c, "ALTER ROLE testrole WITH SUPERUSER")
}

func TestDropRole_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowManageRoles: true})
	assertAllowed(t, c, "DROP ROLE testrole")
}

// --- CREATE EXTENSION Protection ---

func TestCreateExtension_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE EXTENSION pg_trgm", "CREATE EXTENSION is not allowed: can load arbitrary server-side code into PostgreSQL")
}

func TestCreateExtension_IfNotExists(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE EXTENSION IF NOT EXISTS pgcrypto", "CREATE EXTENSION is not allowed")
}

func TestCreateExtension_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCreateExtension: true})
	assertAllowed(t, c, "CREATE EXTENSION pg_trgm")
}

// --- LOCK TABLE Protection ---

func TestLockTable_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "LOCK TABLE users", "LOCK TABLE is not allowed: can acquire exclusive locks causing deadlocks or denial of service")
}

func TestLockTable_ExclusiveMode(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "LOCK TABLE users IN EXCLUSIVE MODE", "LOCK TABLE is not allowed")
}

func TestLockTable_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowLockTable: true})
	assertAllowed(t, c, "LOCK TABLE users")
}

// --- LISTEN / NOTIFY Protection ---

func TestListen_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "LISTEN my_channel", "LISTEN is not allowed: can be used for side-channel communication between sessions")
}

func TestNotify_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "NOTIFY my_channel, 'hello'", "NOTIFY is not allowed: can send arbitrary payloads to listening sessions")
}

func TestNotify_NoPayload(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "NOTIFY my_channel", "NOTIFY is not allowed")
}

func TestListen_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowListenNotify: true})
	assertAllowed(t, c, "LISTEN my_channel")
}

func TestNotify_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowListenNotify: true})
	assertAllowed(t, c, "NOTIFY my_channel, 'hello'")
}

// --- UNLISTEN Protection ---

func TestUnlisten_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "UNLISTEN my_channel", "UNLISTEN is not allowed: managed under the same flag as LISTEN/NOTIFY")
}

func TestUnlisten_All(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "UNLISTEN *", "UNLISTEN is not allowed")
}

func TestUnlisten_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowListenNotify: true})
	assertAllowed(t, c, "UNLISTEN my_channel")
}

// --- Maintenance Command Protection ---

func TestVacuum_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "VACUUM users", "VACUUM/ANALYZE is not allowed: maintenance commands can acquire heavy locks and cause significant I/O load")
}

func TestVacuum_Full(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "VACUUM FULL users", "VACUUM/ANALYZE is not allowed")
}

func TestVacuum_Analyze(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "VACUUM ANALYZE users", "VACUUM/ANALYZE is not allowed")
}

func TestAnalyze_Standalone(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ANALYZE users", "VACUUM/ANALYZE is not allowed")
}

func TestCluster_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CLUSTER users USING users_pkey", "CLUSTER is not allowed: acquires ACCESS EXCLUSIVE lock and rewrites the entire table")
}

func TestReindex_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "REINDEX TABLE users", "REINDEX is not allowed: can acquire ACCESS EXCLUSIVE lock on tables and indexes")
}

func TestReindex_Index(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "REINDEX INDEX users_pkey", "REINDEX is not allowed")
}

func TestVacuum_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMaintenance: true})
	assertAllowed(t, c, "VACUUM users")
}

func TestAnalyze_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMaintenance: true})
	assertAllowed(t, c, "ANALYZE users")
}

func TestCluster_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMaintenance: true})
	assertAllowed(t, c, "CLUSTER users USING users_pkey")
}

func TestReindex_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMaintenance: true})
	assertAllowed(t, c, "REINDEX TABLE users")
}

func TestRefreshMatView_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "REFRESH MATERIALIZED VIEW my_view", "REFRESH MATERIALIZED VIEW is not allowed: can acquire ACCESS EXCLUSIVE lock (without CONCURRENTLY) and cause significant I/O load")
}

func TestRefreshMatView_Concurrently(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "REFRESH MATERIALIZED VIEW CONCURRENTLY my_view", "REFRESH MATERIALIZED VIEW is not allowed")
}

func TestRefreshMatView_WithNoData(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "REFRESH MATERIALIZED VIEW my_view WITH NO DATA", "REFRESH MATERIALIZED VIEW is not allowed")
}

func TestRefreshMatView_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMaintenance: true})
	assertAllowed(t, c, "REFRESH MATERIALIZED VIEW my_view")
}

// --- DDL Protection ---

func TestDDL_CreateTable(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE TABLE test (id int)", "CREATE TABLE is not allowed: DDL operations are blocked")
}

func TestDDL_AlterTable(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER TABLE users ADD COLUMN email text", "ALTER TABLE is not allowed: DDL operations are blocked")
}

func TestDDL_CreateIndex(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE INDEX idx_name ON users (name)", "CREATE INDEX is not allowed: DDL operations are blocked")
}

func TestDDL_CreateSchema(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE SCHEMA myschema", "CREATE SCHEMA is not allowed: DDL operations are blocked")
}

func TestDDL_CreateView(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true", "CREATE VIEW is not allowed: DDL operations are blocked")
}

func TestDDL_CreateSequence(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE SEQUENCE user_id_seq", "CREATE SEQUENCE is not allowed: DDL operations are blocked")
}

func TestDDL_CreateTableAs(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE TABLE summary AS SELECT COUNT(*) FROM users", "CREATE TABLE AS / CREATE MATERIALIZED VIEW is not allowed: DDL operations are blocked")
}

func TestDDL_AlterSequence(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER SEQUENCE user_id_seq RESTART WITH 100", "ALTER SEQUENCE is not allowed: DDL operations are blocked")
}

func TestDDL_Rename(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER TABLE users RENAME TO customers", "RENAME is not allowed: DDL operations are blocked")
}

func TestDDL_CreateTable_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "CREATE TABLE test (id int)")
}

func TestDDL_AlterTable_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "ALTER TABLE users ADD COLUMN email text")
}

func TestDDL_CreateIndex_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "CREATE INDEX idx_name ON users (name)")
}

func TestDDL_DropNotAffectedByDDL(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertBlocked(t, c, "DROP TABLE users", "DROP statements are not allowed")
}

// --- DISCARD Protection ---

func TestDiscard_All(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DISCARD ALL", "DISCARD is not allowed: resets session state including prepared statements and temporary tables")
}

func TestDiscard_Plans(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DISCARD PLANS", "DISCARD is not allowed")
}

func TestDiscard_Temp(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "DISCARD TEMPORARY", "DISCARD is not allowed")
}

func TestDiscard_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDiscard: true})
	assertAllowed(t, c, "DISCARD ALL")
}

// --- COMMENT ON Protection ---

func TestComment_OnTable(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COMMENT ON TABLE users IS 'User accounts'", "COMMENT ON is not allowed: modifies database object metadata")
}

func TestComment_OnColumn(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COMMENT ON COLUMN users.name IS 'Full name'", "COMMENT ON is not allowed")
}

func TestComment_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowComment: true})
	assertAllowed(t, c, "COMMENT ON TABLE users IS 'User accounts'")
}

// --- CREATE TRIGGER Protection ---

func TestCreateTrigger_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE TRIGGER trg_audit AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()", "CREATE TRIGGER is not allowed: triggers execute arbitrary function calls on every DML operation, bypassing protection checks")
}

func TestCreateTrigger_Before(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE TRIGGER trg_before BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION check_func()", "CREATE TRIGGER is not allowed")
}

func TestCreateTrigger_OrReplace(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE OR REPLACE TRIGGER trg_audit AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()", "CREATE TRIGGER is not allowed")
}

func TestCreateTrigger_Statement(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE TRIGGER trg_stmt AFTER INSERT ON users FOR EACH STATEMENT EXECUTE FUNCTION notify_func()", "CREATE TRIGGER is not allowed")
}

func TestCreateTrigger_Constraint(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE CONSTRAINT TRIGGER trg_fk AFTER INSERT ON orders FOR EACH ROW EXECUTE FUNCTION check_fk()", "CREATE TRIGGER is not allowed")
}

func TestCreateTrigger_InsteadOf(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE TRIGGER trg_view INSTEAD OF INSERT ON my_view FOR EACH ROW EXECUTE FUNCTION view_insert()", "CREATE TRIGGER is not allowed")
}

func TestCreateTrigger_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCreateTrigger: true})
	assertAllowed(t, c, "CREATE TRIGGER trg_audit AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()")
}

// --- CREATE RULE Protection ---

func TestCreateRule_Basic(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE RULE notify_insert AS ON INSERT TO users DO ALSO NOTIFY users_changed", "CREATE RULE is not allowed: rules rewrite queries at the parser level, can silently transform statements and bypass protection checks")
}

func TestCreateRule_OrReplace(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE OR REPLACE RULE notify_insert AS ON INSERT TO users DO ALSO NOTIFY users_changed", "CREATE RULE is not allowed")
}

func TestCreateRule_Instead(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE RULE protect_delete AS ON DELETE TO users DO INSTEAD NOTHING", "CREATE RULE is not allowed")
}

func TestCreateRule_WithAction(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "CREATE RULE log_update AS ON UPDATE TO users DO ALSO INSERT INTO audit_log (action) VALUES ('update')", "CREATE RULE is not allowed")
}

func TestCreateRule_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCreateRule: true})
	assertAllowed(t, c, "CREATE RULE notify_insert AS ON INSERT TO users DO ALSO NOTIFY users_changed")
}

// --- ALTER EXTENSION Protection ---

func TestAlterExtension_Update(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER EXTENSION pg_trgm UPDATE TO '1.6'", "ALTER EXTENSION is not allowed: can update extensions, loading new server-side code")
}

func TestAlterExtension_UpdateNoVersion(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER EXTENSION pg_trgm UPDATE", "ALTER EXTENSION is not allowed")
}

func TestAlterExtension_AddTable(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER EXTENSION pg_trgm ADD TABLE my_table", "ALTER EXTENSION is not allowed: can modify extension contents")
}

func TestAlterExtension_DropFunction(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ALTER EXTENSION pg_trgm DROP FUNCTION my_func()", "ALTER EXTENSION is not allowed")
}

func TestAlterExtension_Allowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCreateExtension: true})
	assertAllowed(t, c, "ALTER EXTENSION pg_trgm UPDATE TO '1.6'")
}

func TestAlterExtension_AddAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowCreateExtension: true})
	assertAllowed(t, c, "ALTER EXTENSION pg_trgm ADD TABLE my_table")
}

// --- Transaction Control Protection ---

func TestTransaction_Begin(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "BEGIN", "transaction control statements are not allowed: each query runs in a managed transaction with AfterQuery hooks as guardrails")
}

func TestTransaction_StartTransaction(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "START TRANSACTION", "transaction control statements are not allowed")
}

func TestTransaction_Commit(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COMMIT", "transaction control statements are not allowed")
}

func TestTransaction_End(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "END", "transaction control statements are not allowed")
}

func TestTransaction_Rollback(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ROLLBACK", "transaction control statements are not allowed")
}

func TestTransaction_Abort(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ABORT", "transaction control statements are not allowed")
}

func TestTransaction_Savepoint(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "SAVEPOINT my_savepoint", "transaction control statements are not allowed")
}

func TestTransaction_ReleaseSavepoint(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "RELEASE SAVEPOINT my_savepoint", "transaction control statements are not allowed")
}

func TestTransaction_RollbackToSavepoint(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ROLLBACK TO SAVEPOINT my_savepoint", "transaction control statements are not allowed")
}

func TestTransaction_PrepareTransaction(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "PREPARE TRANSACTION 'my_tx'", "transaction control statements are not allowed")
}

func TestTransaction_CommitPrepared(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "COMMIT PREPARED 'my_tx'", "transaction control statements are not allowed")
}

func TestTransaction_RollbackPrepared(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "ROLLBACK PREPARED 'my_tx'", "transaction control statements are not allowed")
}

func TestTransaction_CannotBeEnabled(t *testing.T) {
	t.Parallel()
	c := NewChecker(allAllowedConfig())
	assertBlocked(t, c, "BEGIN", "transaction control statements are not allowed")
}

func TestTransaction_BeginReadWriteReadOnlyMode(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true})
	assertBlocked(t, c, "BEGIN READ WRITE", "BEGIN READ WRITE is blocked in read-only mode")
}

func TestTransaction_BeginReadOnlyReadOnlyMode(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{ReadOnly: true})
	assertBlocked(t, c, "BEGIN READ ONLY", "transaction control statements are not allowed")
}

// --- Allowed Statements ---

func TestAllowSelect(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT * FROM users")
}

func TestAllowSelectComplex(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH cte AS (SELECT * FROM users) SELECT * FROM cte WHERE id > 1")
}

func TestAllowInsert(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "INSERT INTO users (name) VALUES ('test')")
}

func TestAllowInsertReturning(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "INSERT INTO users (name) VALUES ('test') RETURNING *")
}

func TestAllowInsertOnConflict(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = 'test'")
}

func TestAllowCreateTable(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "CREATE TABLE test (id int)")
}

func TestAllowAlterTable(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowDDL: true})
	assertAllowed(t, c, "ALTER TABLE users ADD COLUMN email text")
}

func TestAllowExplain(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "EXPLAIN ANALYZE SELECT * FROM users")
}

func TestAllowDeleteWithWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "DELETE FROM users WHERE id = 1")
}

func TestAllowUpdateWithWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "UPDATE users SET active = false WHERE id = 1")
}

// --- Complex SQL / Edge Cases ---

func TestCTEWithDelete(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH deleted AS (DELETE FROM users WHERE id = 1 RETURNING *) SELECT * FROM deleted")
}

func TestCTEWithDeleteNoWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "WITH deleted AS (DELETE FROM users RETURNING *) SELECT * FROM deleted", "DELETE without WHERE clause is not allowed")
}

func TestCTEWithUpdateNoWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "WITH updated AS (UPDATE users SET active = false RETURNING *) SELECT * FROM updated", "UPDATE without WHERE clause is not allowed")
}

func TestCTEWithUpdateWithWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH updated AS (UPDATE users SET active = false WHERE id = 1 RETURNING *) SELECT * FROM updated")
}

func TestCTENestedDML(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "WITH a AS (WITH b AS (DELETE FROM users RETURNING *) SELECT * FROM b) SELECT * FROM a", "DELETE without WHERE clause is not allowed")
}

func TestCTEOnInsert(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "WITH src AS (DELETE FROM old_users RETURNING *) INSERT INTO archive SELECT * FROM src", "DELETE without WHERE clause is not allowed")
}

func TestCTEOnUpdate(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH src AS (SELECT id FROM banned) UPDATE users SET active = false WHERE id IN (SELECT id FROM src)")
}

func TestCTEOnDelete(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH src AS (SELECT id FROM banned) DELETE FROM users WHERE id IN (SELECT id FROM src)")
}

func TestCTESelectOnly(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH counts AS (SELECT department, COUNT(*) as cnt FROM employees GROUP BY department) SELECT * FROM counts")
}

func TestCTEMultipleDML(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH d AS (DELETE FROM old_users WHERE expired = true RETURNING *), i AS (INSERT INTO archive SELECT * FROM d RETURNING *) SELECT * FROM i")
}

func TestCTEWithInsert(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH ins AS (INSERT INTO archive (name) VALUES ('test') RETURNING *) SELECT * FROM ins")
}

func TestCTEWithInsertOnConflict(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH ins AS (INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = 'test' RETURNING *) SELECT * FROM ins")
}

// Note: pg_query_go (PostgreSQL parser) accepts MERGE inside CTEs, but PostgreSQL's
// execution engine rejects it at runtime with "MERGE not supported in WITH query".
// These tests verify AST-level protection checking is correct regardless.

func TestCTEWithMergeBlocked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "WITH m AS (MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name) SELECT 1", "MERGE statements are not allowed")
}

func TestCTEWithMergeAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMerge: true})
	assertAllowed(t, c, "WITH m AS (MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name) SELECT 1")
}

func TestCTEOnMerge_DeleteNoWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMerge: true})
	assertBlocked(t, c, "WITH d AS (DELETE FROM users RETURNING *) MERGE INTO target t USING d ON t.id = d.id WHEN MATCHED THEN UPDATE SET name = d.name", "DELETE without WHERE clause is not allowed")
}

func TestCTEOnMerge_SelectCTE(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMerge: true})
	assertAllowed(t, c, "WITH src AS (SELECT * FROM staging) MERGE INTO target t USING src ON t.id = src.id WHEN MATCHED THEN UPDATE SET name = src.name")
}

func TestCTEOnMerge_UpdateNoWhere(t *testing.T) {
	t.Parallel()
	c := NewChecker(Config{AllowMerge: true})
	assertBlocked(t, c, "WITH u AS (UPDATE users SET active = false RETURNING *) MERGE INTO target t USING u ON t.id = u.id WHEN NOT MATCHED THEN INSERT (id) VALUES (u.id)", "UPDATE without WHERE clause is not allowed")
}

func TestNestedSubquerySelect(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT * FROM (SELECT * FROM (SELECT id FROM users) AS a) AS b")
}

func TestComplexJoins(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id LEFT JOIN items i ON o.id = i.order_id WHERE u.active = true")
}

func TestWindowFunction(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees")
}

func TestRecursiveCTE(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "WITH RECURSIVE tree AS (SELECT id, parent_id FROM categories WHERE parent_id IS NULL UNION ALL SELECT c.id, c.parent_id FROM categories c JOIN tree t ON c.parent_id = t.id) SELECT * FROM tree")
}

func TestJSONBQuery(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, `SELECT data->>'name' AS name, data->'address'->>'city' AS city FROM users WHERE data @> '{"active": true}'`)
}

func TestArrayQuery(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT * FROM users WHERE tags @> ARRAY['admin']::text[]")
}

func TestLateralJoin(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT * FROM users u, LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id ORDER BY created_at DESC LIMIT 5) recent_orders")
}

func TestParseError(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "NOT VALID SQL @#$", "SQL parse error")
}

func TestAllProtectionsAllowed(t *testing.T) {
	t.Parallel()
	c := NewChecker(allAllowedConfig())
	assertAllowed(t, c, "DROP TABLE users")
}

func TestSQLInjection_UnionBased(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT * FROM users WHERE id = 1 UNION SELECT * FROM pg_shadow")
}

func TestSQLInjection_CommentBased(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertAllowed(t, c, "SELECT * FROM users -- WHERE admin = true")
}

func TestSQLInjection_MultiStatement(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "SELECT * FROM users; DROP TABLE users", "multi-statement queries are not allowed: found 2 statements")
}

func TestSQLInjection_Stacked(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	// The trailing "-- " is a comment, not a statement, so pg_query sees 2 statements.
	assertBlocked(t, c, "SELECT 1; DELETE FROM users; --", "multi-statement queries are not allowed: found 2 statements")
}

func TestEmptySQL(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "", "SQL parse error: empty query")
}

func TestWhitespaceOnlySQL(t *testing.T) {
	t.Parallel()
	c := NewChecker(defaultConfig())
	assertBlocked(t, c, "   ", "SQL parse error: empty query")
}
