package pgmcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/netip"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Query executes the full query pipeline and returns only QueryOutput.
// All errors (Postgres errors, protection rejections, hook rejections, Go errors)
// are converted to output.Error. The error message is then evaluated against
// error_prompts patterns — any matching prompt messages are appended.
// This means callers only need to check output.Error, never a Go error.
func (p *PostgresMcp) Query(ctx context.Context, input QueryInput) *QueryOutput {
	startTime := time.Now()
	sql := input.SQL

	// 1. Acquire semaphore (respects context cancellation to prevent deadlock)
	select {
	case p.semaphore <- struct{}{}:
	case <-ctx.Done():
		return p.handleError(fmt.Errorf("failed to acquire query slot: all %d connection slots are in use, context cancelled while waiting: %w", cap(p.semaphore), ctx.Err()))
	}
	defer func() { <-p.semaphore }()

	// 2. Check SQL length (before any processing — parsing, hooks, protection)
	if len(sql) > p.config.Query.MaxSQLLength {
		return p.handleError(fmt.Errorf("SQL query too long: %d bytes exceeds maximum of %d bytes", len(sql), p.config.Query.MaxSQLLength))
	}

	// --- Pipeline tracking ---
	var beforeHooks, afterHooks []string
	timeoutRule := ""
	sanitized := false

	// 3. Run BeforeQuery hooks (middleware chain)
	var err error
	if len(p.goBeforeHooks) > 0 {
		sql, err = p.runGoBeforeHooks(ctx, sql)
		for _, entry := range p.goBeforeHooks {
			beforeHooks = append(beforeHooks, entry.Name)
		}
	} else if p.cmdHooks != nil {
		sql, beforeHooks, err = p.cmdHooks.RunBeforeQuery(ctx, sql)
	}
	if err != nil {
		return p.handleError(err)
	}

	// 4. Protection check (on potentially modified query)
	if err := p.protection.Check(sql); err != nil {
		return p.handleError(err)
	}

	// 5. Determine timeout
	var timeout time.Duration
	timeout, timeoutRule = p.timeoutMgr.GetTimeoutWithPattern(sql)
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 6. Acquire connection and execute in transaction
	conn, err := p.pool.Acquire(queryCtx)
	if err != nil {
		return p.handleError(err)
	}
	defer conn.Release()

	tx, err := conn.Begin(queryCtx)
	if err != nil {
		return p.handleError(err)
	}
	defer tx.Rollback(ctx) // use parent ctx, not queryCtx — if query timed out, queryCtx is cancelled and rollback would fail

	rows, err := tx.Query(queryCtx, sql)
	if err != nil {
		return p.handleError(err)
	}

	// 7. Collect results
	result, err := p.collectRows(rows)
	if err != nil {
		return p.handleError(err)
	}

	// 8. Detect read-only vs write statement
	isReadOnly := isReadOnlyStatement(sql)

	// 9. For read-only queries, rollback immediately (no commit needed)
	if isReadOnly {
		tx.Rollback(ctx)
	}

	// 10. AfterQuery hooks — run BEFORE commit for write queries.
	// This allows hooks to reject and trigger rollback for writes.
	var finalResult *QueryOutput
	if len(p.goAfterHooks) > 0 {
		finalResult, err = p.runGoAfterHooks(ctx, result)
		if err != nil {
			return p.handleError(err)
		}
		for _, entry := range p.goAfterHooks {
			afterHooks = append(afterHooks, entry.Name)
		}
	} else if p.cmdHooks != nil && p.cmdHooks.HasAfterQueryHooks() {
		resultJSON, err := json.Marshal(result)
		if err != nil {
			return p.handleError(err)
		}

		modifiedJSON, executed, err := p.cmdHooks.RunAfterQuery(ctx, string(resultJSON))
		if err != nil {
			return p.handleError(err)
		}
		afterHooks = executed

		finalResult = &QueryOutput{}
		dec := json.NewDecoder(strings.NewReader(modifiedJSON))
		dec.UseNumber()
		if err := dec.Decode(finalResult); err != nil {
			return p.handleError(err)
		}
	} else {
		finalResult = result
	}

	// 11. For write queries, commit AFTER hooks have approved the result.
	// Commit uses queryCtx intentionally — ensures entire pipeline completes within query timeout.
	if !isReadOnly {
		if err := tx.Commit(queryCtx); err != nil {
			return p.handleError(err)
		}
	}

	// 12. Apply sanitization (per-field, recursive into JSONB/arrays)
	sanitized = p.sanitizer.HasRules()
	finalResult.Rows = p.sanitizer.SanitizeRows(finalResult.Rows)

	// 13. Apply max result length truncation
	p.truncateIfNeeded(finalResult)

	// 14. Log successful query execution with pipeline details
	logEvent := p.logger.Info().
		Str("sql", truncateForLog(sql, 200)).
		Dur("duration", time.Since(startTime)).
		Int("row_count", len(finalResult.Rows)).
		Int64("rows_affected", finalResult.RowsAffected)
	if len(beforeHooks) > 0 {
		logEvent = logEvent.Strs("before_hooks", beforeHooks)
	}
	if len(afterHooks) > 0 {
		logEvent = logEvent.Strs("after_hooks", afterHooks)
	}
	if timeoutRule != "" {
		logEvent = logEvent.Str("timeout_rule", timeoutRule)
	}
	if sanitized {
		logEvent = logEvent.Bool("sanitized", true)
	}
	logEvent.Msg("query executed")

	return finalResult
}

// isReadOnlyStatement returns true if the SQL is a read-only statement.
// The SQL has already passed protection checks (single statement, parsed successfully).
func isReadOnlyStatement(sql string) bool {
	result, err := pg_query.Parse(sql)
	if err != nil || len(result.Stmts) == 0 {
		return false
	}
	node := result.Stmts[0].Stmt
	switch node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		return true
	case *pg_query.Node_ExplainStmt:
		return true
	case *pg_query.Node_VariableSetStmt:
		return true
	case *pg_query.Node_VariableShowStmt:
		return true
	default:
		return false
	}
}

// runGoBeforeHooks runs Go-interface BeforeQuery hooks in middleware chain.
func (p *PostgresMcp) runGoBeforeHooks(ctx context.Context, sql string) (string, error) {
	for _, entry := range p.goBeforeHooks {
		timeout := entry.Timeout
		if timeout == 0 {
			timeout = time.Duration(p.config.DefaultHookTimeoutSeconds) * time.Second
		}
		hookCtx, cancel := context.WithTimeout(ctx, timeout)

		modified, err := entry.Hook.Run(hookCtx, sql)
		cancel()
		if err != nil {
			if hookCtx.Err() == context.DeadlineExceeded {
				return "", fmt.Errorf("before_query hook error: hook timed out (name: %s, timeout: %s)", entry.Name, timeout)
			}
			return "", fmt.Errorf("before_query hook error: hook rejected query (name: %s): %w", entry.Name, err)
		}
		sql = modified
	}
	return sql, nil
}

// runGoAfterHooks runs Go-interface AfterQuery hooks in middleware chain.
func (p *PostgresMcp) runGoAfterHooks(ctx context.Context, result *QueryOutput) (*QueryOutput, error) {
	for _, entry := range p.goAfterHooks {
		timeout := entry.Timeout
		if timeout == 0 {
			timeout = time.Duration(p.config.DefaultHookTimeoutSeconds) * time.Second
		}
		hookCtx, cancel := context.WithTimeout(ctx, timeout)

		modified, err := entry.Hook.Run(hookCtx, result)
		cancel()
		if err != nil {
			if hookCtx.Err() == context.DeadlineExceeded {
				return nil, fmt.Errorf("after_query hook error: hook timed out (name: %s, timeout: %s)", entry.Name, timeout)
			}
			return nil, fmt.Errorf("after_query hook error: hook rejected result (name: %s): %w", entry.Name, err)
		}
		result = modified
	}
	return result, nil
}

// collectRows reads all rows from pgx.Rows and returns a QueryOutput.
func (p *PostgresMcp) collectRows(rows pgx.Rows) (*QueryOutput, error) {
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		columns[i] = fd.Name
	}

	resultRows := make([]map[string]interface{}, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			row[col] = convertValue(values[i])
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rowsAffected := rows.CommandTag().RowsAffected()

	return &QueryOutput{Columns: columns, Rows: resultRows, RowsAffected: rowsAffected}, nil
}

// convertValue converts a pgx-returned value to a JSON-friendly Go type.
func convertValue(v interface{}) interface{} {
	switch val := v.(type) {
	case nil:
		return nil
	case time.Time:
		return val.Format(time.RFC3339Nano)
	case float32:
		if math.IsNaN(float64(val)) {
			return "NaN"
		}
		if math.IsInf(float64(val), 1) {
			return "Infinity"
		}
		if math.IsInf(float64(val), -1) {
			return "-Infinity"
		}
		return val
	case float64:
		if math.IsNaN(val) {
			return "NaN"
		}
		if math.IsInf(val, 1) {
			return "Infinity"
		}
		if math.IsInf(val, -1) {
			return "-Infinity"
		}
		return val
	case netip.Prefix:
		return val.String()
	case net.HardwareAddr:
		return val.String()
	case pgtype.Time:
		if !val.Valid {
			return nil
		}
		us := val.Microseconds
		hours := us / 3_600_000_000
		us -= hours * 3_600_000_000
		minutes := us / 60_000_000
		us -= minutes * 60_000_000
		seconds := us / 1_000_000
		us -= seconds * 1_000_000
		if us > 0 {
			return fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, us)
		}
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	case pgtype.Interval:
		if !val.Valid {
			return nil
		}
		parts := []string{}
		if val.Months != 0 {
			years := val.Months / 12
			months := val.Months % 12
			if years != 0 {
				parts = append(parts, fmt.Sprintf("%d year(s)", years))
			}
			if months != 0 {
				parts = append(parts, fmt.Sprintf("%d mon(s)", months))
			}
		}
		if val.Days != 0 {
			parts = append(parts, fmt.Sprintf("%d day(s)", val.Days))
		}
		if val.Microseconds != 0 {
			dur := time.Duration(val.Microseconds) * time.Microsecond
			parts = append(parts, dur.String())
		}
		if len(parts) == 0 {
			return "0"
		}
		return strings.Join(parts, " ")
	case pgtype.Numeric:
		if !val.Valid {
			return nil
		}
		if val.NaN {
			return "NaN"
		}
		if val.InfinityModifier == pgtype.Infinity {
			return "Infinity"
		}
		if val.InfinityModifier == pgtype.NegativeInfinity {
			return "-Infinity"
		}
		b, err := val.MarshalJSON()
		if err != nil {
			return nil
		}
		return string(b)
	case pgtype.Range[interface{}]:
		if !val.Valid {
			return nil
		}
		if val.LowerType == pgtype.Empty {
			return "empty"
		}
		var sb strings.Builder
		if val.LowerType == pgtype.Inclusive {
			sb.WriteByte('[')
		} else {
			sb.WriteByte('(')
		}
		if val.LowerType != pgtype.Unbounded {
			sb.WriteString(fmt.Sprintf("%v", convertValue(val.Lower)))
		}
		sb.WriteByte(',')
		if val.UpperType != pgtype.Unbounded {
			sb.WriteString(fmt.Sprintf("%v", convertValue(val.Upper)))
		}
		if val.UpperType == pgtype.Inclusive {
			sb.WriteByte(']')
		} else {
			sb.WriteByte(')')
		}
		return sb.String()
	case pgtype.Point:
		if !val.Valid {
			return nil
		}
		return fmt.Sprintf("(%g,%g)", val.P.X, val.P.Y)
	case pgtype.Line:
		if !val.Valid {
			return nil
		}
		return fmt.Sprintf("{%g,%g,%g}", val.A, val.B, val.C)
	case pgtype.Lseg:
		if !val.Valid {
			return nil
		}
		return fmt.Sprintf("[(%g,%g),(%g,%g)]", val.P[0].X, val.P[0].Y, val.P[1].X, val.P[1].Y)
	case pgtype.Box:
		if !val.Valid {
			return nil
		}
		return fmt.Sprintf("(%g,%g),(%g,%g)", val.P[0].X, val.P[0].Y, val.P[1].X, val.P[1].Y)
	case pgtype.Path:
		if !val.Valid {
			return nil
		}
		points := make([]string, len(val.P))
		for i, p := range val.P {
			points[i] = fmt.Sprintf("(%g,%g)", p.X, p.Y)
		}
		joined := strings.Join(points, ",")
		if val.Closed {
			return "(" + joined + ")"
		}
		return "[" + joined + "]"
	case pgtype.Polygon:
		if !val.Valid {
			return nil
		}
		points := make([]string, len(val.P))
		for i, p := range val.P {
			points[i] = fmt.Sprintf("(%g,%g)", p.X, p.Y)
		}
		return "(" + strings.Join(points, ",") + ")"
	case pgtype.Circle:
		if !val.Valid {
			return nil
		}
		return fmt.Sprintf("<(%g,%g),%g>", val.P.X, val.P.Y, val.R)
	case pgtype.Bits:
		if !val.Valid {
			return nil
		}
		result := make([]byte, val.Len)
		for i := int32(0); i < val.Len; i++ {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			if val.Bytes[byteIdx]&(1<<uint(bitIdx)) != 0 {
				result[i] = '1'
			} else {
				result[i] = '0'
			}
		}
		return string(result)
	case [16]byte:
		// UUID
		return fmt.Sprintf("%x-%x-%x-%x-%x", val[0:4], val[4:6], val[6:8], val[8:10], val[10:16])
	case []byte:
		// bytea, xml — base64 encode
		return base64.StdEncoding.EncodeToString(val)
	case string:
		return val
	case map[string]interface{}:
		result := make(map[string]interface{}, len(val))
		for k, v := range val {
			result[k] = convertValue(v)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, v := range val {
			result[i] = convertValue(v)
		}
		return result
	default:
		return val
	}
}

// handleError converts any error into a QueryOutput with error message.
// The error message is evaluated against error_prompts — matching prompt messages are appended.
func (p *PostgresMcp) handleError(err error) *QueryOutput {
	errMsg := err.Error()
	prompt := p.errPrompts.Match(errMsg)
	patterns := p.errPrompts.MatchedPatterns(errMsg)

	logEvent := p.logger.Error().Err(err)
	if len(patterns) > 0 {
		logEvent = logEvent.Strs("error_prompts", patterns)
	}
	logEvent.Msg("query error")

	if prompt != "" {
		errMsg = errMsg + "\n\n" + prompt
	}
	return &QueryOutput{Error: errMsg}
}

// truncateIfNeeded truncates query output rows if they exceed MaxResultLength (in characters).
func (p *PostgresMcp) truncateIfNeeded(output *QueryOutput) {
	jsonBytes, _ := json.Marshal(output.Rows)
	jsonStr := string(jsonBytes)
	if utf8.RuneCountInString(jsonStr) <= p.config.Query.MaxResultLength {
		return
	}
	// Truncate to MaxResultLength characters (runes)
	runes := []rune(jsonStr)
	truncated := string(runes[:p.config.Query.MaxResultLength])
	output.Rows = nil
	output.Error = truncated + "...[truncated] Result is too long! Add limits in your query!"
}

// truncateForLog truncates a string for log output to avoid oversized log entries.
func truncateForLog(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	truncateAt := maxLen
	for truncateAt > 0 && !utf8.RuneStart(s[truncateAt]) {
		truncateAt--
	}
	return s[:truncateAt] + "...[truncated]"
}
