import { useEffect, useRef, useState } from "react";
import mermaid from "mermaid";
import { ErrorBanner } from "./ResultsTable";
import { Button } from "@/components/ui/button";
import { generateERDSyntax } from "@/lib/schema-queries";
import type { ForeignKeyInfo } from "@/lib/schema-queries";

mermaid.initialize({ startOnLoad: false, theme: "dark" });

let renderCounter = 0;

interface MermaidERDProps {
  tables: { schema: string; name: string }[];
  foreignKeys: ForeignKeyInfo[];
  onTableClick?: (schema: string, table: string) => void;
}

export default function MermaidERD({ tables, foreignKeys, onTableClick }: MermaidERDProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [svgContent, setSvgContent] = useState<string>("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (tables.length === 0) return;

    const syntax = generateERDSyntax(tables, foreignKeys);
    const id = `erd-${++renderCounter}`;

    mermaid.render(id, syntax).then(
      ({ svg }) => {
        setSvgContent(svg);
        setError(null);
      },
      (err) => {
        setError(err instanceof Error ? err.message : String(err));
        setSvgContent("");
      },
    );
  }, [tables, foreignKeys]);

  // Attach click handlers after SVG is inserted
  useEffect(() => {
    if (!containerRef.current || !onTableClick) return;
    const nodes = containerRef.current.querySelectorAll<SVGGElement>("g.entity");
    for (const node of nodes) {
      const text = node.querySelector("text")?.textContent?.trim();
      if (!text) continue;
      node.style.cursor = "pointer";
      node.addEventListener("click", () => {
        // Reverse the sanitization: find matching table
        // Entity names are sanitized versions of schema_table or just table
        // We match against the original tables list
        for (const t of tables) {
          const needsPrefix = t.schema !== "dbo" && t.schema !== "public";
          const raw = needsPrefix ? `${t.schema}_${t.name}` : t.name;
          const sanitized = raw.replace(/[^a-zA-Z0-9_]/g, "_");
          if (sanitized === text) {
            onTableClick(t.schema, t.name);
            return;
          }
        }
      });
    }
  }, [svgContent, tables, onTableClick]);

  const downloadSvg = () => {
    if (!svgContent) return;
    const blob = new Blob([svgContent], { type: "image/svg+xml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "erd-diagram.svg";
    a.click();
    URL.revokeObjectURL(url);
  };

  if (error) return <ErrorBanner message={`ERD render error: ${error}`} />;

  if (tables.length === 0) {
    return <p className="text-muted-foreground text-sm text-center py-8">Select a database to view the ERD</p>;
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex justify-end">
        <Button variant="outline" size="sm" onClick={downloadSvg} disabled={!svgContent}>
          Download SVG
        </Button>
      </div>
      <div
        ref={containerRef}
        className="overflow-auto rounded border border-border bg-card p-4"
        dangerouslySetInnerHTML={{ __html: svgContent }}
      />
    </div>
  );
}
