package pgmcp

// QueryInput is the input for the Query tool.
type QueryInput struct {
	SQL string `json:"sql"`
}

// QueryOutput is the output of the Query tool. All errors (Postgres errors,
// protection rejections, hook rejections, Go errors) are placed in Error.
// The error message is evaluated against error_prompts and matching prompt
// messages are appended.
type QueryOutput struct {
	Columns      []string                 `json:"columns"`
	Rows         []map[string]interface{} `json:"rows"`
	RowsAffected int64                    `json:"rows_affected"`
	Error        string                   `json:"error,omitempty"`
}

// ListTablesInput is the input for the ListTables tool.
type ListTablesInput struct{}

// TableEntry represents a single table/view in the ListTables output.
type TableEntry struct {
	Schema              string `json:"schema"`
	Name                string `json:"name"`
	Type                string `json:"type"` // "table", "view", "materialized_view", "foreign_table", "partitioned_table"
	Owner               string `json:"owner"`
	SchemaAccessLimited bool   `json:"schema_access_limited,omitempty"`
}

// ListTablesOutput is the output of the ListTables tool.
type ListTablesOutput struct {
	Tables []TableEntry `json:"tables"`
	Error  string       `json:"error,omitempty"`
}

// DescribeTableInput is the input for the DescribeTable tool.
type DescribeTableInput struct {
	Table  string `json:"table"`
	Schema string `json:"schema"`
}

// ColumnInfo describes a single column.
type ColumnInfo struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Nullable     bool   `json:"nullable"`
	Default      string `json:"default,omitempty"`
	IsPrimaryKey bool   `json:"is_primary_key"`
}

// IndexInfo describes a single index.
type IndexInfo struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
	IsUnique   bool   `json:"is_unique"`
	IsPrimary  bool   `json:"is_primary"`
}

// ConstraintInfo describes a single constraint.
type ConstraintInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"` // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
	Definition string `json:"definition"`
}

// ForeignKeyInfo describes a single foreign key.
type ForeignKeyInfo struct {
	Name              string `json:"name"`
	Columns           string `json:"columns"`
	ReferencedTable   string `json:"referenced_table"`
	ReferencedColumns string `json:"referenced_columns"`
	OnUpdate          string `json:"on_update"`
	OnDelete          string `json:"on_delete"`
}

// PartitionInfo describes partition metadata.
type PartitionInfo struct {
	Strategy     string   `json:"strategy"`               // "range", "list", "hash"
	PartitionKey string   `json:"partition_key"`           // e.g. "created_at", "region"
	Partitions   []string `json:"partitions,omitempty"`    // child partition table names
	ParentTable  string   `json:"parent_table,omitempty"`  // set if this is a child partition
}

// DescribeTableOutput is the output of the DescribeTable tool.
type DescribeTableOutput struct {
	Schema      string           `json:"schema"`
	Name        string           `json:"name"`
	Type        string           `json:"type"`                  // "table", "view", "materialized_view", "foreign_table", "partitioned_table"
	Definition  string           `json:"definition,omitempty"`  // view/matview SQL definition
	Columns     []ColumnInfo     `json:"columns"`
	Indexes     []IndexInfo      `json:"indexes"`
	Constraints []ConstraintInfo `json:"constraints"`
	ForeignKeys []ForeignKeyInfo `json:"foreign_keys"`
	Partition   *PartitionInfo   `json:"partition,omitempty"`
	Error       string           `json:"error,omitempty"`
}
