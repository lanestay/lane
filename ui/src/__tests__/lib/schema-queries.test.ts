import {
  fetchForeignKeys,
  fetchIndexes,
  getForeignKeysForTable,
  getReferencingForeignKeys,
  generateERDSyntax,
} from "@/lib/schema-queries";
import type { ForeignKeyInfo } from "@/lib/schema-queries";
import { mockFetch, jsonResponse } from "../helpers";

beforeEach(() => {
  localStorage.setItem("session_token", "test-key");
});

afterEach(() => {
  vi.restoreAllMocks();
});

const FK_QUERY_RESULT = {
  success: true,
  total_rows: 2,
  execution_time_ms: 5,
  rows_per_second: 400,
  data: [
    {
      FK_NAME: "FK_orders_users",
      PARENT_TABLE: "orders",
      PARENT_SCHEMA: "dbo",
      PARENT_COLUMN: "user_id",
      REFERENCED_TABLE: "users",
      REFERENCED_SCHEMA: "dbo",
      REFERENCED_COLUMN: "id",
    },
    {
      FK_NAME: "FK_order_items_orders",
      PARENT_TABLE: "order_items",
      PARENT_SCHEMA: "dbo",
      PARENT_COLUMN: "order_id",
      REFERENCED_TABLE: "orders",
      REFERENCED_SCHEMA: "dbo",
      REFERENCED_COLUMN: "id",
    },
  ],
  metadata: { columns: [] },
};

const INDEX_QUERY_RESULT = {
  success: true,
  total_rows: 2,
  execution_time_ms: 5,
  rows_per_second: 400,
  data: [
    {
      INDEX_NAME: "PK_users",
      TABLE_NAME: "users",
      TABLE_SCHEMA: "dbo",
      COLUMN_NAME: "id",
      IS_UNIQUE: true,
    },
    {
      INDEX_NAME: "IX_orders_user_id",
      TABLE_NAME: "orders",
      TABLE_SCHEMA: "dbo",
      COLUMN_NAME: "user_id",
      IS_UNIQUE: false,
    },
  ],
  metadata: { columns: [] },
};

describe("fetchForeignKeys", () => {
  it("calls executeQuery with MSSQL dialect SQL and normalizes results", async () => {
    const spy = mockFetch(async () => jsonResponse(FK_QUERY_RESULT));
    const result = await fetchForeignKeys("master", "mssql", "dev-mssql");

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      FK_NAME: "FK_orders_users",
      PARENT_TABLE: "orders",
      PARENT_SCHEMA: "dbo",
      PARENT_COLUMN: "user_id",
      REFERENCED_TABLE: "users",
      REFERENCED_SCHEMA: "dbo",
      REFERENCED_COLUMN: "id",
    });

    // Verify MSSQL-specific SQL was sent
    const body = JSON.parse(spy.mock.calls[0][1].body);
    expect(body.query).toContain("sys.foreign_keys");
    expect(body.database).toBe("master");
    expect(body.connection).toBe("dev-mssql");
  });

  it("calls executeQuery with Postgres dialect SQL", async () => {
    const spy = mockFetch(async () => jsonResponse(FK_QUERY_RESULT));
    await fetchForeignKeys("postgres", "postgres", "dev-pg");

    const body = JSON.parse(spy.mock.calls[0][1].body);
    expect(body.query).toContain("information_schema.table_constraints");
    expect(body.query).toContain("FOREIGN KEY");
  });

  it("normalizes lowercase column names from result rows", async () => {
    const lowercaseResult = {
      ...FK_QUERY_RESULT,
      data: [
        {
          fk_name: "FK_test",
          parent_table: "t1",
          parent_schema: "dbo",
          parent_column: "col1",
          referenced_table: "t2",
          referenced_schema: "dbo",
          referenced_column: "id",
        },
      ],
    };
    mockFetch(async () => jsonResponse(lowercaseResult));
    const result = await fetchForeignKeys("master", "mssql");

    expect(result[0].FK_NAME).toBe("FK_test");
    expect(result[0].PARENT_TABLE).toBe("t1");
  });
});

describe("fetchIndexes", () => {
  it("calls executeQuery with MSSQL dialect SQL and normalizes results", async () => {
    const spy = mockFetch(async () => jsonResponse(INDEX_QUERY_RESULT));
    const result = await fetchIndexes("master", "mssql", "dev-mssql");

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      INDEX_NAME: "PK_users",
      TABLE_NAME: "users",
      TABLE_SCHEMA: "dbo",
      COLUMN_NAME: "id",
      IS_UNIQUE: true,
    });
    expect(result[1].IS_UNIQUE).toBe(false);

    const body = JSON.parse(spy.mock.calls[0][1].body);
    expect(body.query).toContain("sys.indexes");
  });

  it("calls executeQuery with Postgres dialect SQL", async () => {
    const spy = mockFetch(async () => jsonResponse(INDEX_QUERY_RESULT));
    await fetchIndexes("postgres", "postgres");

    const body = JSON.parse(spy.mock.calls[0][1].body);
    expect(body.query).toContain("pg_index");
  });

  it("handles IS_UNIQUE as integer (1/0) from MSSQL", async () => {
    const intResult = {
      ...INDEX_QUERY_RESULT,
      data: [
        { INDEX_NAME: "IX_test", TABLE_NAME: "t", TABLE_SCHEMA: "dbo", COLUMN_NAME: "c", IS_UNIQUE: 1 },
        { INDEX_NAME: "IX_test2", TABLE_NAME: "t", TABLE_SCHEMA: "dbo", COLUMN_NAME: "c2", IS_UNIQUE: 0 },
      ],
    };
    mockFetch(async () => jsonResponse(intResult));
    const result = await fetchIndexes("master", "mssql");
    expect(result[0].IS_UNIQUE).toBe(true);
    expect(result[1].IS_UNIQUE).toBe(false);
  });
});

describe("getForeignKeysForTable", () => {
  const fks: ForeignKeyInfo[] = [
    { FK_NAME: "FK1", PARENT_TABLE: "orders", PARENT_SCHEMA: "dbo", PARENT_COLUMN: "user_id", REFERENCED_TABLE: "users", REFERENCED_SCHEMA: "dbo", REFERENCED_COLUMN: "id" },
    { FK_NAME: "FK2", PARENT_TABLE: "items", PARENT_SCHEMA: "dbo", PARENT_COLUMN: "order_id", REFERENCED_TABLE: "orders", REFERENCED_SCHEMA: "dbo", REFERENCED_COLUMN: "id" },
    { FK_NAME: "FK3", PARENT_TABLE: "orders", PARENT_SCHEMA: "sales", PARENT_COLUMN: "prod_id", REFERENCED_TABLE: "products", REFERENCED_SCHEMA: "dbo", REFERENCED_COLUMN: "id" },
  ];

  it("returns outgoing FKs for a table", () => {
    const result = getForeignKeysForTable(fks, "dbo", "orders");
    expect(result).toHaveLength(1);
    expect(result[0].FK_NAME).toBe("FK1");
  });

  it("matches schema correctly", () => {
    const result = getForeignKeysForTable(fks, "sales", "orders");
    expect(result).toHaveLength(1);
    expect(result[0].FK_NAME).toBe("FK3");
  });

  it("returns empty for table with no outgoing FKs", () => {
    expect(getForeignKeysForTable(fks, "dbo", "users")).toHaveLength(0);
  });
});

describe("getReferencingForeignKeys", () => {
  const fks: ForeignKeyInfo[] = [
    { FK_NAME: "FK1", PARENT_TABLE: "orders", PARENT_SCHEMA: "dbo", PARENT_COLUMN: "user_id", REFERENCED_TABLE: "users", REFERENCED_SCHEMA: "dbo", REFERENCED_COLUMN: "id" },
    { FK_NAME: "FK2", PARENT_TABLE: "items", PARENT_SCHEMA: "dbo", PARENT_COLUMN: "order_id", REFERENCED_TABLE: "orders", REFERENCED_SCHEMA: "dbo", REFERENCED_COLUMN: "id" },
  ];

  it("returns incoming FKs for a table", () => {
    const result = getReferencingForeignKeys(fks, "dbo", "users");
    expect(result).toHaveLength(1);
    expect(result[0].FK_NAME).toBe("FK1");
  });

  it("returns empty for table with no incoming FKs", () => {
    expect(getReferencingForeignKeys(fks, "dbo", "items")).toHaveLength(0);
  });
});

describe("generateERDSyntax", () => {
  it("produces valid Mermaid erDiagram syntax", () => {
    const tables = [
      { schema: "dbo", name: "users" },
      { schema: "dbo", name: "orders" },
    ];
    const fks: ForeignKeyInfo[] = [
      { FK_NAME: "FK_orders_users", PARENT_TABLE: "orders", PARENT_SCHEMA: "dbo", PARENT_COLUMN: "user_id", REFERENCED_TABLE: "users", REFERENCED_SCHEMA: "dbo", REFERENCED_COLUMN: "id" },
    ];
    const result = generateERDSyntax(tables, fks);

    expect(result).toContain("erDiagram");
    expect(result).toContain("users {");
    expect(result).toContain("orders {");
    expect(result).toContain('orders }o--|| users : "FK_orders_users"');
  });

  it("deduplicates composite FK relationships by FK_NAME", () => {
    const tables = [
      { schema: "dbo", name: "a" },
      { schema: "dbo", name: "b" },
    ];
    const fks: ForeignKeyInfo[] = [
      { FK_NAME: "FK_composite", PARENT_TABLE: "a", PARENT_SCHEMA: "dbo", PARENT_COLUMN: "col1", REFERENCED_TABLE: "b", REFERENCED_SCHEMA: "dbo", REFERENCED_COLUMN: "id1" },
      { FK_NAME: "FK_composite", PARENT_TABLE: "a", PARENT_SCHEMA: "dbo", PARENT_COLUMN: "col2", REFERENCED_TABLE: "b", REFERENCED_SCHEMA: "dbo", REFERENCED_COLUMN: "id2" },
    ];
    const result = generateERDSyntax(tables, fks);
    const relationshipLines = result.split("\n").filter((l) => l.includes("}o--||"));
    expect(relationshipLines).toHaveLength(1);
  });

  it("prefixes entity names with schema when not dbo/public", () => {
    const tables = [{ schema: "sales", name: "orders" }];
    const result = generateERDSyntax(tables, []);
    expect(result).toContain("sales_orders {");
  });

  it("omits schema prefix for dbo and public", () => {
    const tables = [
      { schema: "dbo", name: "t1" },
      { schema: "public", name: "t2" },
    ];
    const result = generateERDSyntax(tables, []);
    expect(result).toContain("t1 {");
    expect(result).toContain("t2 {");
    expect(result).not.toContain("dbo_");
    expect(result).not.toContain("public_");
  });

  it("sanitizes special characters in table names", () => {
    const tables = [{ schema: "dbo", name: "my-table.v2" }];
    const result = generateERDSyntax(tables, []);
    expect(result).toContain("my_table_v2 {");
  });

  it("renders isolated entities when no FKs exist", () => {
    const tables = [
      { schema: "dbo", name: "a" },
      { schema: "dbo", name: "b" },
    ];
    const result = generateERDSyntax(tables, []);
    expect(result).toContain("a {");
    expect(result).toContain("b {");
    expect(result).not.toContain("}o--||");
  });
});
