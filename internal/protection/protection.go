package protection

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Config is the protection checker's own config type.
type Config struct {
	AllowSet                bool
	AllowDrop               bool
	AllowTruncate           bool
	AllowDo                 bool
	AllowCopyFrom           bool
	AllowCopyTo             bool
	AllowCreateFunction     bool
	AllowPrepare            bool
	AllowDeleteWithoutWhere bool
	AllowUpdateWithoutWhere bool
	AllowAlterSystem        bool
	AllowMerge              bool
	AllowGrantRevoke        bool
	AllowManageRoles        bool
	AllowCreateExtension    bool
	AllowLockTable          bool
	AllowListenNotify       bool
	AllowMaintenance        bool
	AllowDDL                bool
	AllowDiscard            bool
	AllowComment            bool
	AllowCreateTrigger      bool
	AllowCreateRule         bool
	ReadOnly                bool
}

// Checker validates SQL statements against protection rules.
type Checker struct {
	config Config
}

// NewChecker creates a new Checker with the given config.
func NewChecker(config Config) *Checker {
	return &Checker{config: config}
}

// Check parses SQL with pg_query_go and walks the AST.
// Returns nil if allowed, descriptive error if blocked.
func (c *Checker) Check(sql string) error {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return fmt.Errorf("SQL parse error: %w", err)
	}

	if len(result.Stmts) == 0 {
		return fmt.Errorf("SQL parse error: empty query")
	}

	if len(result.Stmts) > 1 {
		return fmt.Errorf("multi-statement queries are not allowed: found %d statements", len(result.Stmts))
	}

	for _, rawStmt := range result.Stmts {
		if err := c.checkNode(rawStmt.Stmt); err != nil {
			return err
		}
	}
	return nil
}

// checkNode recursively checks a single AST node and its CTEs against protection rules.
func (c *Checker) checkNode(node *pg_query.Node) error {
	if node == nil {
		return nil
	}

	if err := c.checkCTEs(node); err != nil {
		return err
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_VariableSetStmt:
		varSetStmt := n.VariableSetStmt

		if c.config.ReadOnly {
			if varSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET_ALL {
				return fmt.Errorf("RESET ALL is blocked in read-only mode: could disable read-only transaction setting")
			}
			if varSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET &&
				isTransactionReadOnlyVar(varSetStmt.Name) {
				return fmt.Errorf("RESET %s is blocked in read-only mode", varSetStmt.Name)
			}
			if isTransactionReadOnlyVar(varSetStmt.Name) {
				return fmt.Errorf("SET %s is blocked in read-only mode: cannot change transaction read-only setting", varSetStmt.Name)
			}
		}
		if !c.config.AllowSet {
			switch varSetStmt.Kind {
			case pg_query.VariableSetKind_VAR_RESET_ALL:
				return fmt.Errorf("RESET ALL is not allowed")
			case pg_query.VariableSetKind_VAR_RESET:
				return fmt.Errorf("RESET statements are not allowed: RESET %s", varSetStmt.Name)
			default:
				return fmt.Errorf("SET statements are not allowed: SET %s", varSetStmt.Name)
			}
		}

	case *pg_query.Node_DropStmt:
		if !c.config.AllowDrop {
			return fmt.Errorf("DROP statements are not allowed")
		}

	case *pg_query.Node_DropdbStmt:
		if !c.config.AllowDrop {
			return fmt.Errorf("DROP DATABASE is not allowed")
		}

	case *pg_query.Node_TruncateStmt:
		if !c.config.AllowTruncate {
			return fmt.Errorf("TRUNCATE statements are not allowed")
		}

	case *pg_query.Node_DoStmt:
		if !c.config.AllowDo {
			return fmt.Errorf("DO $$ blocks are not allowed: DO blocks can execute arbitrary SQL bypassing protection checks")
		}

	case *pg_query.Node_DeleteStmt:
		if !c.config.AllowDeleteWithoutWhere && n.DeleteStmt.WhereClause == nil {
			return fmt.Errorf("DELETE without WHERE clause is not allowed")
		}

	case *pg_query.Node_UpdateStmt:
		if !c.config.AllowUpdateWithoutWhere && n.UpdateStmt.WhereClause == nil {
			return fmt.Errorf("UPDATE without WHERE clause is not allowed")
		}

	case *pg_query.Node_MergeStmt:
		if !c.config.AllowMerge {
			return fmt.Errorf("MERGE statements are not allowed: MERGE can perform INSERT, UPDATE, and DELETE operations bypassing individual DML protection rules")
		}

	case *pg_query.Node_CopyStmt:
		if !c.config.AllowCopyFrom && n.CopyStmt.IsFrom {
			return fmt.Errorf("COPY FROM is not allowed")
		}
		if !c.config.AllowCopyTo && !n.CopyStmt.IsFrom {
			return fmt.Errorf("COPY TO is not allowed: can export/exfiltrate data from tables")
		}

	case *pg_query.Node_CreateFunctionStmt:
		if !c.config.AllowCreateFunction {
			if n.CreateFunctionStmt.IsProcedure {
				return fmt.Errorf("CREATE PROCEDURE is not allowed: can contain arbitrary SQL bypassing protection checks")
			}
			return fmt.Errorf("CREATE FUNCTION is not allowed: can contain arbitrary SQL bypassing protection checks")
		}

	case *pg_query.Node_PrepareStmt:
		if !c.config.AllowPrepare {
			return fmt.Errorf("PREPARE statements are not allowed: prepared statements can be executed later bypassing protection checks")
		}

	case *pg_query.Node_ExplainStmt:
		if n.ExplainStmt.Query != nil {
			if err := c.checkNode(n.ExplainStmt.Query); err != nil {
				return err
			}
		}

	case *pg_query.Node_AlterSystemStmt:
		if !c.config.AllowAlterSystem {
			return fmt.Errorf("ALTER SYSTEM is not allowed: can modify server-level configuration (shared_preload_libraries, archive_command, ssl, etc.)")
		}

	case *pg_query.Node_GrantStmt:
		if !c.config.AllowGrantRevoke {
			if n.GrantStmt.IsGrant {
				return fmt.Errorf("GRANT statements are not allowed: can modify database permissions")
			}
			return fmt.Errorf("REVOKE statements are not allowed: can modify database permissions")
		}

	case *pg_query.Node_GrantRoleStmt:
		if !c.config.AllowGrantRevoke {
			if n.GrantRoleStmt.IsGrant {
				return fmt.Errorf("GRANT ROLE is not allowed: can modify role memberships")
			}
			return fmt.Errorf("REVOKE ROLE is not allowed: can modify role memberships")
		}

	case *pg_query.Node_CreateRoleStmt:
		if !c.config.AllowManageRoles {
			return fmt.Errorf("CREATE ROLE/USER is not allowed: can create database roles with privileges")
		}

	case *pg_query.Node_AlterRoleStmt:
		if !c.config.AllowManageRoles {
			return fmt.Errorf("ALTER ROLE/USER is not allowed: can modify role privileges including SUPERUSER")
		}

	case *pg_query.Node_DropRoleStmt:
		if !c.config.AllowManageRoles {
			return fmt.Errorf("DROP ROLE/USER is not allowed: can delete database roles")
		}

	case *pg_query.Node_CreateExtensionStmt:
		if !c.config.AllowCreateExtension {
			return fmt.Errorf("CREATE EXTENSION is not allowed: can load arbitrary server-side code into PostgreSQL")
		}

	case *pg_query.Node_LockStmt:
		if !c.config.AllowLockTable {
			return fmt.Errorf("LOCK TABLE is not allowed: can acquire exclusive locks causing deadlocks or denial of service")
		}

	case *pg_query.Node_ListenStmt:
		if !c.config.AllowListenNotify {
			return fmt.Errorf("LISTEN is not allowed: can be used for side-channel communication between sessions")
		}

	case *pg_query.Node_NotifyStmt:
		if !c.config.AllowListenNotify {
			return fmt.Errorf("NOTIFY is not allowed: can send arbitrary payloads to listening sessions")
		}

	case *pg_query.Node_VacuumStmt:
		if !c.config.AllowMaintenance {
			return fmt.Errorf("VACUUM/ANALYZE is not allowed: maintenance commands can acquire heavy locks and cause significant I/O load")
		}

	case *pg_query.Node_ClusterStmt:
		if !c.config.AllowMaintenance {
			return fmt.Errorf("CLUSTER is not allowed: acquires ACCESS EXCLUSIVE lock and rewrites the entire table")
		}

	case *pg_query.Node_ReindexStmt:
		if !c.config.AllowMaintenance {
			return fmt.Errorf("REINDEX is not allowed: can acquire ACCESS EXCLUSIVE lock on tables and indexes")
		}

	case *pg_query.Node_CreateStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("CREATE TABLE is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_AlterTableStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("ALTER TABLE is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_IndexStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("CREATE INDEX is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_CreateSchemaStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("CREATE SCHEMA is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_ViewStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("CREATE VIEW is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_CreateSeqStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("CREATE SEQUENCE is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_CreateTableAsStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("CREATE TABLE AS / CREATE MATERIALIZED VIEW is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_AlterSeqStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("ALTER SEQUENCE is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_RenameStmt:
		if !c.config.AllowDDL {
			return fmt.Errorf("RENAME is not allowed: DDL operations are blocked")
		}

	case *pg_query.Node_DiscardStmt:
		if !c.config.AllowDiscard {
			return fmt.Errorf("DISCARD is not allowed: resets session state including prepared statements and temporary tables")
		}

	case *pg_query.Node_CommentStmt:
		if !c.config.AllowComment {
			return fmt.Errorf("COMMENT ON is not allowed: modifies database object metadata")
		}

	case *pg_query.Node_CreateTrigStmt:
		if !c.config.AllowCreateTrigger {
			return fmt.Errorf("CREATE TRIGGER is not allowed: triggers execute arbitrary function calls on every DML operation, bypassing protection checks")
		}

	case *pg_query.Node_RuleStmt:
		if !c.config.AllowCreateRule {
			return fmt.Errorf("CREATE RULE is not allowed: rules rewrite queries at the parser level, can silently transform statements and bypass protection checks")
		}

	case *pg_query.Node_RefreshMatViewStmt:
		if !c.config.AllowMaintenance {
			return fmt.Errorf("REFRESH MATERIALIZED VIEW is not allowed: can acquire ACCESS EXCLUSIVE lock (without CONCURRENTLY) and cause significant I/O load")
		}

	case *pg_query.Node_AlterExtensionStmt:
		if !c.config.AllowCreateExtension {
			return fmt.Errorf("ALTER EXTENSION is not allowed: can update extensions, loading new server-side code")
		}

	case *pg_query.Node_AlterExtensionContentsStmt:
		if !c.config.AllowCreateExtension {
			return fmt.Errorf("ALTER EXTENSION is not allowed: can modify extension contents")
		}

	case *pg_query.Node_AlterRoleSetStmt:
		if !c.config.AllowManageRoles {
			return fmt.Errorf("ALTER ROLE/USER is not allowed: can modify role privileges including SUPERUSER")
		}

	case *pg_query.Node_TransactionStmt:
		if c.config.ReadOnly {
			txStmt := n.TransactionStmt
			for _, opt := range txStmt.Options {
				if defElem, ok := opt.Node.(*pg_query.Node_DefElem); ok {
					if defElem.DefElem.Defname == "transaction_read_only" {
						// In pg_query_go v6, the arg is AConst with Ival.
						// Ival == 0 means READ WRITE (false).
						if aconst, ok := defElem.DefElem.Arg.Node.(*pg_query.Node_AConst); ok {
							if ival, ok := aconst.AConst.Val.(*pg_query.A_Const_Ival); ok {
								if ival.Ival.Ival == 0 { // 0 = false = READ WRITE
									return fmt.Errorf("BEGIN READ WRITE is blocked in read-only mode: cannot start a read-write transaction")
								}
							}
						}
					}
				}
			}
		}
		return fmt.Errorf("transaction control statements are not allowed: each query runs in a managed transaction with AfterQuery hooks as guardrails")
	}
	return nil
}

// checkCTEs extracts the WITH clause from a node (if any) and recursively
// checks each CTE's subquery.
func (c *Checker) checkCTEs(node *pg_query.Node) error {
	var withClause *pg_query.WithClause
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		withClause = n.SelectStmt.WithClause
	case *pg_query.Node_InsertStmt:
		withClause = n.InsertStmt.WithClause
	case *pg_query.Node_UpdateStmt:
		withClause = n.UpdateStmt.WithClause
	case *pg_query.Node_DeleteStmt:
		withClause = n.DeleteStmt.WithClause
	case *pg_query.Node_MergeStmt:
		withClause = n.MergeStmt.WithClause
	}
	if withClause == nil {
		return nil
	}
	for _, cte := range withClause.Ctes {
		cteNode, ok := cte.Node.(*pg_query.Node_CommonTableExpr)
		if !ok {
			continue
		}
		if err := c.checkNode(cteNode.CommonTableExpr.Ctequery); err != nil {
			return err
		}
	}
	return nil
}

func isTransactionReadOnlyVar(name string) bool {
	return name == "default_transaction_read_only" || name == "transaction_read_only"
}
