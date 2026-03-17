import { useState, useEffect } from "react";
import { getOperatorsForType, type ColumnFilter, type FilterOperator } from "../lib/sql-gen";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

const OPERATOR_LABELS: Record<FilterOperator, string> = {
  contains: "contains",
  equals: "equals",
  not_equals: "not equals",
  starts_with: "starts with",
  ends_with: "ends with",
  greater_than: ">",
  less_than: "<",
  greater_or_equal: ">=",
  less_or_equal: "<=",
  is_null: "is null",
  is_not_null: "is not null",
};

const NULLARY_OPS = new Set<FilterOperator>(["is_null", "is_not_null"]);

interface ColumnFilterPopoverProps {
  columnName: string;
  dataType: string;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  currentFilter: ColumnFilter | undefined;
  onApply: (filter: ColumnFilter) => void;
  onClear: () => void;
  children: React.ReactNode;
}

export default function ColumnFilterPopover({
  columnName,
  dataType,
  isOpen,
  onOpenChange,
  currentFilter,
  onApply,
  onClear,
  children,
}: ColumnFilterPopoverProps) {
  const operators = getOperatorsForType(dataType);
  const [operator, setOperator] = useState<FilterOperator>(currentFilter?.operator ?? operators[0]);
  const [value, setValue] = useState(currentFilter?.value ?? "");

  // Reset when popover opens
  useEffect(() => {
    if (isOpen) {
      setOperator(currentFilter?.operator ?? operators[0]);
      setValue(currentFilter?.value ?? "");
    }
  }, [isOpen, currentFilter, operators]);

  const apply = () => {
    onApply({ column: columnName, operator, value, dataType });
    onOpenChange(false);
  };

  const clear = () => {
    onClear();
    onOpenChange(false);
  };

  return (
    <Popover open={isOpen} onOpenChange={onOpenChange}>
      <PopoverTrigger asChild>
        {children}
      </PopoverTrigger>
      <PopoverContent className="w-64 p-3" align="start">
        <div className="flex flex-col gap-2">
          <p className="text-xs font-medium text-muted-foreground">
            Filter: {columnName}
          </p>
          <Select value={operator} onValueChange={(v) => setOperator(v as FilterOperator)}>
            <SelectTrigger size="sm" className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {operators.map((op) => (
                <SelectItem key={op} value={op}>
                  {OPERATOR_LABELS[op]}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {!NULLARY_OPS.has(operator) && (
            <Input
              className="h-8 text-sm"
              placeholder="Value..."
              value={value}
              onChange={(e) => setValue(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") apply();
              }}
              autoFocus
            />
          )}
          <div className="flex justify-between pt-1">
            <Button variant="ghost" size="sm" onClick={clear}>
              Clear
            </Button>
            <Button size="sm" onClick={apply}>
              Apply
            </Button>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
