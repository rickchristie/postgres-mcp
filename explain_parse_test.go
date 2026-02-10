package pgmcp_test

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// These tests verify pg_query_go's parsing behavior for EXPLAIN with
// various statement types. PostgreSQL's grammar only allows certain
// statements inside EXPLAIN (ExplainableStmt). This test confirms
// which statements produce parse errors vs. valid ASTs, ensuring our
// protection checker test expectations are correct.

func TestExplainParse_DropTable(t *testing.T) {
	// DROP is NOT an ExplainableStmt in PostgreSQL grammar.
	// Expected: parse error.
	_, err := pg_query.Parse("EXPLAIN DROP TABLE users")
	if err == nil {
		t.Fatal("expected parse error for EXPLAIN DROP TABLE, but got nil")
	}
	t.Logf("EXPLAIN DROP TABLE parse error: %v", err)
}

func TestExplainParse_AnalyzeDropTable(t *testing.T) {
	// EXPLAIN ANALYZE DROP — same issue.
	_, err := pg_query.Parse("EXPLAIN ANALYZE DROP TABLE users")
	if err == nil {
		t.Fatal("expected parse error for EXPLAIN ANALYZE DROP TABLE, but got nil")
	}
	t.Logf("EXPLAIN ANALYZE DROP TABLE parse error: %v", err)
}

func TestExplainParse_Truncate(t *testing.T) {
	// TRUNCATE is NOT an ExplainableStmt.
	_, err := pg_query.Parse("EXPLAIN ANALYZE TRUNCATE users")
	if err == nil {
		t.Fatal("expected parse error for EXPLAIN ANALYZE TRUNCATE, but got nil")
	}
	t.Logf("EXPLAIN ANALYZE TRUNCATE parse error: %v", err)
}

func TestExplainParse_DeleteWithoutWhere(t *testing.T) {
	// DELETE IS an ExplainableStmt. Should parse successfully.
	result, err := pg_query.Parse("EXPLAIN DELETE FROM users")
	if err != nil {
		t.Fatalf("expected successful parse for EXPLAIN DELETE, got error: %v", err)
	}
	if len(result.Stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(result.Stmts))
	}
	node := result.Stmts[0].Stmt.Node
	explainStmt, ok := node.(*pg_query.Node_ExplainStmt)
	if !ok {
		t.Fatalf("expected ExplainStmt, got %T", node)
	}
	inner := explainStmt.ExplainStmt.Query.Node
	_, isDelete := inner.(*pg_query.Node_DeleteStmt)
	if !isDelete {
		t.Fatalf("expected inner DeleteStmt, got %T", inner)
	}
	t.Log("EXPLAIN DELETE FROM users: parsed successfully, inner is DeleteStmt")
}

func TestExplainParse_AnalyzeDeleteWithoutWhere(t *testing.T) {
	// EXPLAIN ANALYZE DELETE — should parse and have ExplainStmt wrapping DeleteStmt.
	result, err := pg_query.Parse("EXPLAIN ANALYZE DELETE FROM users")
	if err != nil {
		t.Fatalf("expected successful parse, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	explainStmt, ok := node.(*pg_query.Node_ExplainStmt)
	if !ok {
		t.Fatalf("expected ExplainStmt, got %T", node)
	}
	inner := explainStmt.ExplainStmt.Query.Node
	_, isDelete := inner.(*pg_query.Node_DeleteStmt)
	if !isDelete {
		t.Fatalf("expected inner DeleteStmt, got %T", inner)
	}
	t.Log("EXPLAIN ANALYZE DELETE FROM users: parsed successfully")
}

func TestExplainParse_UpdateWithoutWhere(t *testing.T) {
	// UPDATE IS an ExplainableStmt.
	result, err := pg_query.Parse("EXPLAIN ANALYZE UPDATE users SET active = false")
	if err != nil {
		t.Fatalf("expected successful parse, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	explainStmt, ok := node.(*pg_query.Node_ExplainStmt)
	if !ok {
		t.Fatalf("expected ExplainStmt, got %T", node)
	}
	inner := explainStmt.ExplainStmt.Query.Node
	_, isUpdate := inner.(*pg_query.Node_UpdateStmt)
	if !isUpdate {
		t.Fatalf("expected inner UpdateStmt, got %T", inner)
	}
	t.Log("EXPLAIN ANALYZE UPDATE: parsed successfully")
}

func TestExplainParse_Select(t *testing.T) {
	// SELECT IS an ExplainableStmt.
	result, err := pg_query.Parse("EXPLAIN ANALYZE SELECT * FROM users")
	if err != nil {
		t.Fatalf("expected successful parse, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_ExplainStmt)
	if !ok {
		t.Fatalf("expected ExplainStmt, got %T", node)
	}
	t.Log("EXPLAIN ANALYZE SELECT: parsed successfully")
}

func TestExplainParse_Insert(t *testing.T) {
	// INSERT IS an ExplainableStmt.
	result, err := pg_query.Parse("EXPLAIN ANALYZE INSERT INTO users (name) VALUES ('test')")
	if err != nil {
		t.Fatalf("expected successful parse, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_ExplainStmt)
	if !ok {
		t.Fatalf("expected ExplainStmt, got %T", node)
	}
	t.Log("EXPLAIN ANALYZE INSERT: parsed successfully")
}

func TestExplainParse_CTEWithDelete(t *testing.T) {
	// CTE with DELETE inside EXPLAIN — should parse, inner is SelectStmt with WithClause.
	sql := "EXPLAIN ANALYZE WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	explainStmt, ok := node.(*pg_query.Node_ExplainStmt)
	if !ok {
		t.Fatalf("expected ExplainStmt, got %T", node)
	}
	inner := explainStmt.ExplainStmt.Query.Node
	selectStmt, isSelect := inner.(*pg_query.Node_SelectStmt)
	if !isSelect {
		t.Fatalf("expected inner SelectStmt, got %T", inner)
	}
	if selectStmt.SelectStmt.WithClause == nil {
		t.Fatal("expected WithClause on SelectStmt")
	}
	ctes := selectStmt.SelectStmt.WithClause.Ctes
	if len(ctes) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(ctes))
	}
	cteNode, ok := ctes[0].Node.(*pg_query.Node_CommonTableExpr)
	if !ok {
		t.Fatalf("expected CommonTableExpr, got %T", ctes[0].Node)
	}
	cteQuery := cteNode.CommonTableExpr.Ctequery.Node
	_, isDelete := cteQuery.(*pg_query.Node_DeleteStmt)
	if !isDelete {
		t.Fatalf("expected CTE query to be DeleteStmt, got %T", cteQuery)
	}
	t.Log("EXPLAIN ANALYZE WITH d AS (DELETE ...) SELECT ...: parsed successfully, CTE contains DeleteStmt")
}

// Test MERGE statement parsing (PostgreSQL 15+)
func TestMergeParse_Basic(t *testing.T) {
	sql := `MERGE INTO target t
		USING source s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET name = s.name
		WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)`
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for MERGE, got error: %v", err)
	}
	if len(result.Stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(result.Stmts))
	}
	node := result.Stmts[0].Stmt.Node
	mergeStmt, ok := node.(*pg_query.Node_MergeStmt)
	if !ok {
		t.Fatalf("expected MergeStmt, got %T", node)
	}
	t.Logf("MERGE parsed successfully. MergeStmt type: %T", mergeStmt)
	t.Logf("MERGE node details: %+v", mergeStmt.MergeStmt)
}

func TestMergeParse_WithDelete(t *testing.T) {
	sql := `MERGE INTO target t
		USING source s ON t.id = s.id
		WHEN MATCHED THEN DELETE`
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for MERGE with DELETE, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_MergeStmt)
	if !ok {
		t.Fatalf("expected MergeStmt, got %T", node)
	}
	t.Log("MERGE with DELETE: parsed successfully")
}

func TestExplainParse_Merge(t *testing.T) {
	// MERGE is an ExplainableStmt in PostgreSQL 15+.
	sql := `EXPLAIN ANALYZE MERGE INTO target t
		USING source s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET name = s.name
		WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)`
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for EXPLAIN MERGE, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	explainStmt, ok := node.(*pg_query.Node_ExplainStmt)
	if !ok {
		t.Fatalf("expected ExplainStmt, got %T", node)
	}
	inner := explainStmt.ExplainStmt.Query.Node
	_, isMerge := inner.(*pg_query.Node_MergeStmt)
	if !isMerge {
		t.Fatalf("expected inner MergeStmt, got %T", inner)
	}
	t.Log("EXPLAIN ANALYZE MERGE: parsed successfully, inner is MergeStmt")
}

// Test privilege-related statements parsing
func TestGrantParse(t *testing.T) {
	sql := "GRANT SELECT ON users TO readonly_user"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for GRANT, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_GrantStmt)
	if !ok {
		t.Fatalf("expected GrantStmt, got %T", node)
	}
	t.Logf("GRANT: parsed as GrantStmt")
}

func TestRevokeParse(t *testing.T) {
	sql := "REVOKE SELECT ON users FROM readonly_user"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for REVOKE, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	grantStmt, ok := node.(*pg_query.Node_GrantStmt)
	if !ok {
		t.Fatalf("expected GrantStmt (REVOKE is also GrantStmt), got %T", node)
	}
	// REVOKE is parsed as GrantStmt with is_grant = false
	t.Logf("REVOKE: parsed as GrantStmt, is_grant=%v", grantStmt.GrantStmt.IsGrant)
}

func TestGrantRoleParse(t *testing.T) {
	sql := "GRANT admin TO bob"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for GRANT role, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_GrantRoleStmt)
	if !ok {
		t.Fatalf("expected GrantRoleStmt, got %T", node)
	}
	t.Log("GRANT role: parsed as GrantRoleStmt")
}

func TestRevokeRoleParse(t *testing.T) {
	sql := "REVOKE admin FROM bob"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for REVOKE role, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_GrantRoleStmt)
	if !ok {
		t.Fatalf("expected GrantRoleStmt (REVOKE role is also GrantRoleStmt), got %T", node)
	}
	t.Log("REVOKE role: parsed as GrantRoleStmt")
}

func TestCreateRoleParse(t *testing.T) {
	sql := "CREATE ROLE testrole WITH LOGIN PASSWORD 'secret'"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for CREATE ROLE, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_CreateRoleStmt)
	if !ok {
		t.Fatalf("expected CreateRoleStmt, got %T", node)
	}
	t.Log("CREATE ROLE: parsed as CreateRoleStmt")
}

func TestCreateUserParse(t *testing.T) {
	// CREATE USER is syntactic sugar for CREATE ROLE ... LOGIN
	sql := "CREATE USER testuser WITH PASSWORD 'secret'"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for CREATE USER, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_CreateRoleStmt)
	if !ok {
		t.Fatalf("expected CreateRoleStmt (CREATE USER is also CreateRoleStmt), got %T", node)
	}
	t.Log("CREATE USER: parsed as CreateRoleStmt")
}

func TestAlterRoleParse(t *testing.T) {
	sql := "ALTER ROLE testrole WITH SUPERUSER"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for ALTER ROLE, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_AlterRoleStmt)
	if !ok {
		t.Fatalf("expected AlterRoleStmt, got %T", node)
	}
	t.Log("ALTER ROLE: parsed as AlterRoleStmt")
}

func TestDropRoleParse(t *testing.T) {
	sql := "DROP ROLE testrole"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for DROP ROLE, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_DropRoleStmt)
	if !ok {
		t.Fatalf("expected DropRoleStmt, got %T", node)
	}
	t.Log("DROP ROLE: parsed as DropRoleStmt")
}

func TestDropUserParse(t *testing.T) {
	// DROP USER is syntactic sugar for DROP ROLE
	sql := "DROP USER testuser"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for DROP USER, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	_, ok := node.(*pg_query.Node_DropRoleStmt)
	if !ok {
		t.Fatalf("expected DropRoleStmt (DROP USER is also DropRoleStmt), got %T", node)
	}
	t.Log("DROP USER: parsed as DropRoleStmt")
}

// Test COPY TO parsing to verify AST node type
func TestCopyToParse(t *testing.T) {
	sql := "COPY users TO STDOUT"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse for COPY TO, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	copyStmt, ok := node.(*pg_query.Node_CopyStmt)
	if !ok {
		t.Fatalf("expected CopyStmt, got %T", node)
	}
	if copyStmt.CopyStmt.IsFrom {
		t.Fatal("expected IsFrom=false for COPY TO, but got true")
	}
	t.Logf("COPY TO: parsed as CopyStmt, IsFrom=%v", copyStmt.CopyStmt.IsFrom)
}

func TestCopyToWithQuery(t *testing.T) {
	sql := "COPY (SELECT * FROM users) TO STDOUT"
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("expected successful parse, got error: %v", err)
	}
	node := result.Stmts[0].Stmt.Node
	copyStmt, ok := node.(*pg_query.Node_CopyStmt)
	if !ok {
		t.Fatalf("expected CopyStmt, got %T", node)
	}
	if copyStmt.CopyStmt.IsFrom {
		t.Fatal("expected IsFrom=false for COPY (SELECT ...) TO, but got true")
	}
	t.Log("COPY (SELECT ...) TO STDOUT: parsed successfully, IsFrom=false")
}
