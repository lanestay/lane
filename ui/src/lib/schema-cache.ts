import { listTables, describeTable } from "./api";
import type { ColumnInfo } from "./api";
import type { CompletionSource, CompletionContext, CompletionResult } from "@codemirror/autocomplete";

interface CachedTable {
  schema: string;
  columns: ColumnInfo[] | null; // null = not yet loaded
}

const tableCache = new Map<string, Map<string, CachedTable>>(); // key = "connection:database"

function cacheKey(connection: string, database: string): string {
  return `${connection}:${database}`;
}

export async function loadSchema(connection: string, database: string): Promise<void> {
  const key = cacheKey(connection, database);
  if (tableCache.has(key)) return;

  try {
    const tables = await listTables(database, connection || undefined);
    const map = new Map<string, CachedTable>();
    for (const t of tables) {
      map.set(t.TABLE_NAME, { schema: t.TABLE_SCHEMA, columns: null });
    }
    tableCache.set(key, map);
  } catch {
    // Silently fail — autocomplete will just not have schema data
  }
}

async function loadColumns(
  connection: string,
  database: string,
  tableName: string,
  schema?: string,
): Promise<ColumnInfo[]> {
  const key = cacheKey(connection, database);
  const tables = tableCache.get(key);
  if (!tables) return [];

  const entry = tables.get(tableName);
  if (entry?.columns) return entry.columns;

  try {
    const cols = await describeTable(database, tableName, connection || undefined, schema);
    if (entry) entry.columns = cols;
    return cols;
  } catch {
    return [];
  }
}

export function clearSchema(connection?: string, database?: string): void {
  if (connection && database) {
    tableCache.delete(cacheKey(connection, database));
  } else {
    tableCache.clear();
  }
}

export function createCompletionSource(
  connection: string,
  database: string,
): CompletionSource {
  return async (context: CompletionContext): Promise<CompletionResult | null> => {
    const key = cacheKey(connection, database);
    const tables = tableCache.get(key);
    if (!tables) return null;

    // Check for "tableName." pattern for column completions
    const dotMatch = context.matchBefore(/\w+\.\w*/);
    if (dotMatch) {
      const dotIdx = dotMatch.text.indexOf(".");
      const tableName = dotMatch.text.slice(0, dotIdx);
      const entry = tables.get(tableName);
      if (entry) {
        const cols = await loadColumns(connection, database, tableName, entry.schema);
        if (cols.length === 0) return null;
        return {
          from: dotMatch.from + dotIdx + 1,
          options: cols.map((c) => ({
            label: c.COLUMN_NAME,
            type: "property",
            detail: c.DATA_TYPE,
          })),
        };
      }
    }

    // Table name completions
    const word = context.matchBefore(/\w+/);
    if (!word && !context.explicit) return null;

    return {
      from: word?.from ?? context.pos,
      options: Array.from(tables.entries()).map(([name, info]) => ({
        label: name,
        type: "class",
        detail: info.schema,
      })),
    };
  };
}
