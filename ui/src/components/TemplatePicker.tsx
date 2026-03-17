import { getTemplatesForDialect } from "../lib/templates";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface Props {
  dialect?: string;
  onSelect: (sql: string) => void;
}

export default function TemplatePicker({ dialect, onSelect }: Props) {
  const filtered = getTemplatesForDialect(dialect);

  return (
    <Select
      value=""
      onValueChange={(idx) => {
        const t = filtered[Number(idx)];
        if (t) onSelect(t.sql);
      }}
    >
      <SelectTrigger className="w-[200px]">
        <SelectValue placeholder="Insert template..." />
      </SelectTrigger>
      <SelectContent>
        {filtered.map((t, i) => (
          <SelectItem key={`${t.name}-${t.dialects.join()}`} value={String(i)}>
            <span className="flex flex-col">
              <span>{t.name}</span>
              <span className="text-xs text-muted-foreground">{t.description}</span>
            </span>
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
