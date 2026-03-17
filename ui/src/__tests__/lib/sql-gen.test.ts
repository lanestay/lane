import {
  quoteIdentifier,
  formatLiteral,
  isNumericType,
  hasPrimaryKey,
  getPkColumns,
  generateUpdate,
  escapeLikeValue,
  getOperatorsForType,
  generateWhereClause,
  buildPreviewQuery,
  type CellChange,
  type Dialect,
  type ColumnFilter,
  type SortSpec,
} from "@/lib/sql-gen";
import type { ColumnInfo } from "@/lib/api";

// ---------------------------------------------------------------------------
// quoteIdentifier
// ---------------------------------------------------------------------------

describe("quoteIdentifier", () => {
  it("wraps with brackets for mssql", () => {
    expect(quoteIdentifier("users", "mssql")).toBe("[users]");
  });

  it("wraps with double quotes for postgres", () => {
    expect(quoteIdentifier("users", "postgres")).toBe('"users"');
  });

  it("escapes ] in mssql", () => {
    expect(quoteIdentifier("col]name", "mssql")).toBe("[col]]name]");
  });

  it('escapes " in postgres', () => {
    expect(quoteIdentifier('col"name', "postgres")).toBe('"col""name"');
  });
});

// ---------------------------------------------------------------------------
// formatLiteral
// ---------------------------------------------------------------------------

describe("formatLiteral", () => {
  it("returns NULL for null", () => {
    expect(formatLiteral(null)).toBe("NULL");
  });

  it("returns NULL for undefined", () => {
    expect(formatLiteral(undefined)).toBe("NULL");
  });

  it("returns number unquoted", () => {
    expect(formatLiteral(42)).toBe("42");
    expect(formatLiteral(3.14)).toBe("3.14");
  });

  it("returns boolean as 1/0", () => {
    expect(formatLiteral(true)).toBe("1");
    expect(formatLiteral(false)).toBe("0");
  });

  it("wraps strings in single quotes", () => {
    expect(formatLiteral("hello")).toBe("'hello'");
  });

  it("escapes single quotes in strings", () => {
    expect(formatLiteral("it's")).toBe("'it''s'");
    expect(formatLiteral("O'Brien's")).toBe("'O''Brien''s'");
  });

  it("leaves numeric strings unquoted when dataType is numeric", () => {
    expect(formatLiteral("42", "int")).toBe("42");
    expect(formatLiteral("3.14", "decimal")).toBe("3.14");
  });

  it("quotes non-numeric strings even with numeric dataType", () => {
    expect(formatLiteral("hello", "int")).toBe("'hello'");
    expect(formatLiteral("", "int")).toBe("''");
  });
});

// ---------------------------------------------------------------------------
// isNumericType
// ---------------------------------------------------------------------------

describe("isNumericType", () => {
  it.each([
    "int", "bigint", "smallint", "tinyint",
    "decimal", "numeric", "float", "real",
    "money", "smallmoney", "serial", "bigserial",
    "double precision",
  ])("returns true for %s", (type) => {
    expect(isNumericType(type)).toBe(true);
  });

  it.each([
    "varchar", "nvarchar", "text", "date", "datetime", "bit", "uniqueidentifier",
  ])("returns false for %s", (type) => {
    expect(isNumericType(type)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hasPrimaryKey / getPkColumns
// ---------------------------------------------------------------------------

describe("hasPrimaryKey / getPkColumns", () => {
  const cols: ColumnInfo[] = [
    { COLUMN_NAME: "id", DATA_TYPE: "int", IS_NULLABLE: "NO", IS_PRIMARY_KEY: "YES" },
    { COLUMN_NAME: "name", DATA_TYPE: "varchar", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "NO" },
  ];

  const noPkCols: ColumnInfo[] = [
    { COLUMN_NAME: "col1", DATA_TYPE: "varchar", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "NO" },
  ];

  it("returns true when PK exists", () => {
    expect(hasPrimaryKey(cols)).toBe(true);
  });

  it("returns false when no PK", () => {
    expect(hasPrimaryKey(noPkCols)).toBe(false);
  });

  it("returns PK columns only", () => {
    const pks = getPkColumns(cols);
    expect(pks).toHaveLength(1);
    expect(pks[0].COLUMN_NAME).toBe("id");
  });
});

// ---------------------------------------------------------------------------
// generateUpdate
// ---------------------------------------------------------------------------

describe("generateUpdate", () => {
  const columns: ColumnInfo[] = [
    { COLUMN_NAME: "id", DATA_TYPE: "int", IS_NULLABLE: "NO", IS_PRIMARY_KEY: "YES" },
    { COLUMN_NAME: "name", DATA_TYPE: "varchar", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "NO" },
    { COLUMN_NAME: "age", DATA_TYPE: "int", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "NO" },
  ];

  const rowData = { id: 1, name: "Alice", age: 30 };

  it("generates MSSQL UPDATE with single column change", () => {
    const changes = new Map<string, CellChange>([
      ["name", { original: "Alice", current: "Bob" }],
    ]);
    const sql = generateUpdate("dbo", "users", columns, rowData, changes, "mssql");
    expect(sql).toBe("UPDATE [dbo].[users] SET [name] = 'Bob' WHERE [id] = 1");
  });

  it("generates Postgres UPDATE with single column change", () => {
    const changes = new Map<string, CellChange>([
      ["name", { original: "Alice", current: "Bob" }],
    ]);
    const sql = generateUpdate("public", "users", columns, rowData, changes, "postgres");
    expect(sql).toBe('UPDATE "public"."users" SET "name" = \'Bob\' WHERE "id" = 1');
  });

  it("handles multi-column changes", () => {
    const changes = new Map<string, CellChange>([
      ["name", { original: "Alice", current: "Bob" }],
      ["age", { original: 30, current: 31 }],
    ]);
    const sql = generateUpdate("dbo", "users", columns, rowData, changes, "mssql");
    expect(sql).toBe("UPDATE [dbo].[users] SET [name] = 'Bob', [age] = 31 WHERE [id] = 1");
  });

  it("handles NULL in SET clause", () => {
    const changes = new Map<string, CellChange>([
      ["name", { original: "Alice", current: null }],
    ]);
    const sql = generateUpdate("dbo", "users", columns, rowData, changes, "mssql");
    expect(sql).toBe("UPDATE [dbo].[users] SET [name] = NULL WHERE [id] = 1");
  });

  it("handles NULL PK value with IS NULL in WHERE", () => {
    const nullPkColumns: ColumnInfo[] = [
      { COLUMN_NAME: "id", DATA_TYPE: "int", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "YES" },
      { COLUMN_NAME: "val", DATA_TYPE: "varchar", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "NO" },
    ];
    const changes = new Map<string, CellChange>([
      ["val", { original: "old", current: "new" }],
    ]);
    const sql = generateUpdate("dbo", "t", nullPkColumns, { id: null, val: "old" }, changes, "mssql");
    expect(sql).toBe("UPDATE [dbo].[t] SET [val] = 'new' WHERE [id] IS NULL");
  });

  it("handles composite primary key", () => {
    const compositeCols: ColumnInfo[] = [
      { COLUMN_NAME: "tenant_id", DATA_TYPE: "int", IS_NULLABLE: "NO", IS_PRIMARY_KEY: "YES" },
      { COLUMN_NAME: "user_id", DATA_TYPE: "int", IS_NULLABLE: "NO", IS_PRIMARY_KEY: "YES" },
      { COLUMN_NAME: "role", DATA_TYPE: "varchar", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "NO" },
    ];
    const changes = new Map<string, CellChange>([
      ["role", { original: "user", current: "admin" }],
    ]);
    const row = { tenant_id: 1, user_id: 42, role: "user" };
    const sql = generateUpdate("dbo", "user_roles", compositeCols, row, changes, "mssql");
    expect(sql).toBe("UPDATE [dbo].[user_roles] SET [role] = 'admin' WHERE [tenant_id] = 1 AND [user_id] = 42");
  });

  it("throws when no primary key", () => {
    const noPk: ColumnInfo[] = [
      { COLUMN_NAME: "val", DATA_TYPE: "varchar", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "NO" },
    ];
    const changes = new Map<string, CellChange>([["val", { original: "a", current: "b" }]]);
    expect(() => generateUpdate("dbo", "t", noPk, { val: "a" }, changes, "mssql")).toThrow("no primary key");
  });

  it("throws when no changes", () => {
    const changes = new Map<string, CellChange>();
    expect(() => generateUpdate("dbo", "users", columns, rowData, changes, "mssql")).toThrow("no changes");
  });

  it("escapes single quotes in SET values", () => {
    const changes = new Map<string, CellChange>([
      ["name", { original: "Alice", current: "O'Brien" }],
    ]);
    const sql = generateUpdate("dbo", "users", columns, rowData, changes, "mssql");
    expect(sql).toBe("UPDATE [dbo].[users] SET [name] = 'O''Brien' WHERE [id] = 1");
  });
});

// ---------------------------------------------------------------------------
// escapeLikeValue
// ---------------------------------------------------------------------------

describe("escapeLikeValue", () => {
  it("escapes percent", () => {
    expect(escapeLikeValue("100%")).toBe("100\\%");
  });

  it("escapes underscore", () => {
    expect(escapeLikeValue("user_name")).toBe("user\\_name");
  });

  it("escapes bracket", () => {
    expect(escapeLikeValue("[test]")).toBe("\\[test]");
  });

  it("escapes backslash", () => {
    expect(escapeLikeValue("a\\b")).toBe("a\\\\b");
  });

  it("escapes multiple special chars", () => {
    expect(escapeLikeValue("50% off [deal]")).toBe("50\\% off \\[deal]");
  });

  it("leaves normal text unchanged", () => {
    expect(escapeLikeValue("hello world")).toBe("hello world");
  });
});

// ---------------------------------------------------------------------------
// getOperatorsForType
// ---------------------------------------------------------------------------

describe("getOperatorsForType", () => {
  it("returns string operators for varchar", () => {
    const ops = getOperatorsForType("varchar");
    expect(ops).toContain("contains");
    expect(ops).toContain("starts_with");
    expect(ops).not.toContain("greater_than");
  });

  it("returns numeric operators for int", () => {
    const ops = getOperatorsForType("int");
    expect(ops).toContain("greater_than");
    expect(ops).toContain("less_than");
    expect(ops).not.toContain("contains");
  });

  it("returns all operators for unknown types", () => {
    const ops = getOperatorsForType("datetime");
    expect(ops).toContain("contains");
    expect(ops).toContain("greater_than");
  });
});

// ---------------------------------------------------------------------------
// generateWhereClause
// ---------------------------------------------------------------------------

describe("generateWhereClause", () => {
  const cols = ["id", "name", "email"];

  it("returns empty string when no filters", () => {
    expect(generateWhereClause("", [], "mssql", cols)).toBe("");
  });

  it("generates global search with LIKE for MSSQL", () => {
    const clause = generateWhereClause("alice", [], "mssql", cols);
    expect(clause).toContain("CAST([id] AS VARCHAR(MAX)) LIKE '%alice%'");
    expect(clause).toContain("CAST([name] AS VARCHAR(MAX)) LIKE '%alice%'");
    expect(clause).toContain(" OR ");
  });

  it("generates global search with ILIKE for Postgres", () => {
    const clause = generateWhereClause("alice", [], "postgres", cols);
    expect(clause).toContain('"id"::TEXT ILIKE \'%alice%\'');
    expect(clause).toContain('"name"::TEXT ILIKE \'%alice%\'');
  });

  it("generates column filter expression", () => {
    const filters: ColumnFilter[] = [
      { column: "name", operator: "equals", value: "Alice", dataType: "varchar" },
    ];
    const clause = generateWhereClause("", filters, "mssql", cols);
    expect(clause).toBe("[name] = 'Alice'");
  });

  it("combines global search and column filter with AND", () => {
    const filters: ColumnFilter[] = [
      { column: "name", operator: "equals", value: "Alice", dataType: "varchar" },
    ];
    const clause = generateWhereClause("test", filters, "mssql", cols);
    expect(clause).toContain(") AND [name] = 'Alice'");
  });

  it("handles is_null operator", () => {
    const filters: ColumnFilter[] = [
      { column: "email", operator: "is_null", value: "", dataType: "varchar" },
    ];
    expect(generateWhereClause("", filters, "mssql", cols)).toBe("[email] IS NULL");
  });

  it("handles contains filter with LIKE for MSSQL", () => {
    const filters: ColumnFilter[] = [
      { column: "name", operator: "contains", value: "ali", dataType: "varchar" },
    ];
    expect(generateWhereClause("", filters, "mssql", cols)).toBe("[name] LIKE '%ali%'");
  });

  it("handles contains filter with ILIKE for Postgres", () => {
    const filters: ColumnFilter[] = [
      { column: "name", operator: "contains", value: "ali", dataType: "varchar" },
    ];
    expect(generateWhereClause("", filters, "postgres", cols)).toBe('"name"::TEXT ILIKE \'%ali%\'');
  });
});

// ---------------------------------------------------------------------------
// buildPreviewQuery
// ---------------------------------------------------------------------------

describe("buildPreviewQuery", () => {
  it("generates base MSSQL query with no filters", () => {
    const sql = buildPreviewQuery("dbo", "users", "mssql", "", [], null, []);
    expect(sql).toBe("SELECT TOP 100 * FROM [dbo].[users]");
  });

  it("generates base Postgres query with no filters", () => {
    const sql = buildPreviewQuery("public", "users", "postgres", "", [], null, []);
    expect(sql).toBe('SELECT * FROM "public"."users" LIMIT 100');
  });

  it("includes WHERE clause for global search", () => {
    const sql = buildPreviewQuery("dbo", "users", "mssql", "alice", [], null, ["id", "name"]);
    expect(sql).toContain("WHERE");
    expect(sql).toContain("LIKE '%alice%'");
  });

  it("includes ORDER BY for sort", () => {
    const sort: SortSpec = { column: "name", direction: "ASC" };
    const sql = buildPreviewQuery("dbo", "users", "mssql", "", [], sort, []);
    expect(sql).toBe("SELECT TOP 100 * FROM [dbo].[users] ORDER BY [name] ASC");
  });

  it("includes WHERE and ORDER BY together", () => {
    const filters: ColumnFilter[] = [
      { column: "name", operator: "equals", value: "Alice", dataType: "varchar" },
    ];
    const sort: SortSpec = { column: "id", direction: "DESC" };
    const sql = buildPreviewQuery("dbo", "users", "mssql", "", filters, sort, ["id", "name"]);
    expect(sql).toContain("WHERE [name] = 'Alice'");
    expect(sql).toContain("ORDER BY [id] DESC");
  });

  it("respects custom limit", () => {
    const sql = buildPreviewQuery("dbo", "users", "mssql", "", [], null, [], 50);
    expect(sql).toBe("SELECT TOP 50 * FROM [dbo].[users]");
  });

  it("Postgres puts LIMIT at end after ORDER BY", () => {
    const sort: SortSpec = { column: "id", direction: "ASC" };
    const sql = buildPreviewQuery("public", "users", "postgres", "", [], sort, []);
    expect(sql).toBe('SELECT * FROM "public"."users" ORDER BY "id" ASC LIMIT 100');
  });
});
