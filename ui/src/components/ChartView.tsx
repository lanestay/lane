import { useState, useMemo, useEffect } from "react";
import type { QueryResult } from "../lib/api";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  ScatterChart,
  Scatter,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import type { PieLabelRenderProps } from "recharts";
import { Button } from "@/components/ui/button";

type ChartType = "bar" | "line" | "scatter" | "pie";

const NUMERIC_TYPES = new Set([
  "int",
  "bigint",
  "float",
  "decimal",
  "numeric",
  "money",
  "real",
  "double",
  "smallint",
  "tinyint",
  "smallmoney",
  "int4",
  "int8",
  "int2",
  "float4",
  "float8",
  "serial",
  "bigserial",
]);

const COLORS = [
  "#6366f1",
  "#f59e0b",
  "#10b981",
  "#ef4444",
  "#8b5cf6",
  "#ec4899",
  "#14b8a6",
  "#f97316",
];

const TICK_STYLE = { fontSize: 11, fill: "hsl(var(--muted-foreground))" };
const GRID_COLOR = "hsl(var(--border) / 0.5)";
const TOOLTIP_STYLE = {
  backgroundColor: "hsl(var(--popover))",
  border: "1px solid hsl(var(--border))",
  borderRadius: 8,
  fontSize: 12,
  color: "hsl(var(--popover-foreground))",
};

function isNumericType(type: string): boolean {
  const lower = type.toLowerCase();
  return NUMERIC_TYPES.has(lower) || lower.startsWith("numeric") || lower.startsWith("decimal");
}

export default function ChartView({ result }: { result: QueryResult }) {
  const [chartType, setChartType] = useState<ChartType>("bar");
  const [xColumn, setXColumn] = useState("");
  const [yColumns, setYColumns] = useState<string[]>([]);

  const { numericCols, nonNumericCols } = useMemo(() => {
    const cols = result.metadata?.columns ?? [];
    const numeric: string[] = [];
    const nonNumeric: string[] = [];
    for (const col of cols) {
      if (isNumericType(col.type)) {
        numeric.push(col.name);
      } else {
        nonNumeric.push(col.name);
      }
    }
    // Fallback: if no metadata, try to infer from data
    if (cols.length === 0 && result.data.length > 0) {
      for (const key of Object.keys(result.data[0])) {
        const sample = result.data.find((r) => r[key] != null)?.[key];
        if (typeof sample === "number") {
          numeric.push(key);
        } else {
          nonNumeric.push(key);
        }
      }
    }
    return { numericCols: numeric, nonNumericCols: nonNumeric };
  }, [result]);

  // Auto-select defaults when result changes
  useEffect(() => {
    setXColumn(nonNumericCols[0] ?? numericCols[0] ?? "");
    setYColumns(numericCols.length > 0 ? [numericCols[0]] : []);
  }, [numericCols, nonNumericCols]);

  const allColumns = useMemo(() => {
    if (result.metadata?.columns) return result.metadata.columns.map((c) => c.name);
    if (result.data.length > 0) return Object.keys(result.data[0]);
    return [];
  }, [result]);

  const toggleYColumn = (col: string) => {
    setYColumns((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]
    );
  };

  // Filter out null values for chart data
  const chartData = useMemo(() => {
    return result.data.filter((row) => {
      if (row[xColumn] == null) return false;
      return yColumns.some((y) => row[y] != null);
    });
  }, [result.data, xColumn, yColumns]);

  if (numericCols.length === 0) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground text-sm">
        No numeric columns to chart
      </div>
    );
  }

  const chartTypes: { value: ChartType; label: string }[] = [
    { value: "bar", label: "Bar" },
    { value: "line", label: "Line" },
    { value: "scatter", label: "Scatter" },
    { value: "pie", label: "Pie" },
  ];

  return (
    <div className="flex flex-col gap-3 h-full">
      {/* Toolbar */}
      <div className="flex items-center gap-4 flex-wrap">
        {/* Chart type buttons */}
        <div className="flex items-center gap-1">
          {chartTypes.map((ct) => (
            <Button
              key={ct.value}
              variant={chartType === ct.value ? "secondary" : "ghost"}
              size="sm"
              onClick={() => setChartType(ct.value)}
            >
              {ct.label}
            </Button>
          ))}
        </div>

        {/* X-axis picker */}
        <div className="flex items-center gap-1.5">
          <span className="text-xs text-muted-foreground">X:</span>
          <select
            value={xColumn}
            onChange={(e) => setXColumn(e.target.value)}
            className="bg-muted text-foreground text-xs rounded px-2 py-1 border border-border"
          >
            {allColumns.map((col) => (
              <option key={col} value={col}>
                {col}
              </option>
            ))}
          </select>
        </div>

        {/* Y-axis multi-select */}
        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-xs text-muted-foreground">Y:</span>
          {numericCols.map((col, i) => (
            <button
              key={col}
              onClick={() => toggleYColumn(col)}
              className={`text-xs px-2 py-0.5 rounded border transition-colors ${
                yColumns.includes(col)
                  ? "border-transparent text-white"
                  : "border-border text-muted-foreground hover:text-foreground"
              }`}
              style={
                yColumns.includes(col)
                  ? { backgroundColor: COLORS[i % COLORS.length] }
                  : undefined
              }
            >
              {col}
            </button>
          ))}
        </div>
      </div>

      {/* Chart */}
      <div className="flex-1 min-h-0" style={{ minHeight: 300 }}>
        <ResponsiveContainer width="100%" height="100%">
          {chartType === "bar" ? (
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
              <XAxis dataKey={xColumn} tick={TICK_STYLE} />
              <YAxis tick={TICK_STYLE} />
              <Tooltip contentStyle={TOOLTIP_STYLE} labelStyle={{ color: "hsl(var(--popover-foreground))" }} />
              <Legend wrapperStyle={{ color: "hsl(var(--foreground))" }} />
              {yColumns.map((col) => (
                <Bar
                  key={col}
                  dataKey={col}
                  fill={COLORS[numericCols.indexOf(col) % COLORS.length]}
                  radius={[2, 2, 0, 0]}
                />
              ))}
            </BarChart>
          ) : chartType === "line" ? (
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
              <XAxis dataKey={xColumn} tick={TICK_STYLE} />
              <YAxis tick={TICK_STYLE} />
              <Tooltip contentStyle={TOOLTIP_STYLE} labelStyle={{ color: "hsl(var(--popover-foreground))" }} />
              <Legend wrapperStyle={{ color: "hsl(var(--foreground))" }} />
              {yColumns.map((col) => (
                <Line
                  key={col}
                  type="monotone"
                  dataKey={col}
                  stroke={COLORS[numericCols.indexOf(col) % COLORS.length]}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                />
              ))}
            </LineChart>
          ) : chartType === "scatter" ? (
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
              <XAxis
                dataKey={xColumn}
                name={xColumn}
                type="number"
                tick={TICK_STYLE}
              />
              <YAxis
                dataKey={yColumns[0] ?? ""}
                name={yColumns[0] ?? ""}
                tick={TICK_STYLE}
              />
              <Tooltip contentStyle={TOOLTIP_STYLE} labelStyle={{ color: "hsl(var(--popover-foreground))" }} />
              <Scatter data={chartData} fill={COLORS[0]} />
            </ScatterChart>
          ) : (
            <PieChart>
              <Tooltip contentStyle={TOOLTIP_STYLE} labelStyle={{ color: "hsl(var(--popover-foreground))" }} />
              <Legend wrapperStyle={{ color: "hsl(var(--foreground))" }} />
              <Pie
                data={chartData}
                dataKey={yColumns[0] ?? ""}
                nameKey={xColumn}
                cx="50%"
                cy="50%"
                outerRadius="80%"
                label={(props: PieLabelRenderProps) => {
                  const x = Number(props.x ?? 0);
                  const y = Number(props.y ?? 0);
                  const cx = Number(props.cx ?? 0);
                  return (
                    <text x={x} y={y} fill="hsl(var(--foreground))" textAnchor={x > cx ? "start" : "end"} dominantBaseline="central" fontSize={11}>
                      {String(props.name ?? "")}
                    </text>
                  );
                }}
              >
                {chartData.map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
            </PieChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  );
}
