import { useState, useEffect, useCallback } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  storageListConnections,
  storageListBuckets,
  storageExportQuery,
  workspaceExportToStorage,
} from "../lib/api";
import type { BucketInfo } from "../lib/api";

type ExportMode = "query" | "workspace";

interface SaveToStorageDialogProps {
  open: boolean;
  onClose: () => void;
  /** For "query" mode: the DB connection, database, and query to export */
  query?: string;
  database?: string;
  connection?: string;
  /** Set to "workspace" to export from workspace DuckDB instead of a DB query */
  mode?: ExportMode;
  onSuccess?: (result: { key: string; size: number; row_count: number; format: string }) => void;
  onError?: (msg: string) => void;
}

export default function SaveToStorageDialog({
  open,
  onClose,
  query,
  database,
  connection,
  mode = "query",
  onSuccess,
  onError,
}: SaveToStorageDialogProps) {
  const [storageConnections, setStorageConnections] = useState<string[]>([]);
  const [selectedConnection, setSelectedConnection] = useState("");
  const [buckets, setBuckets] = useState<BucketInfo[]>([]);
  const [selectedBucket, setSelectedBucket] = useState("");
  const [key, setKey] = useState("");
  const [format, setFormat] = useState("csv");
  const [saving, setSaving] = useState(false);

  // Load storage connections
  useEffect(() => {
    if (!open) return;
    storageListConnections()
      .then((conns) => {
        setStorageConnections(conns);
        if (conns.length === 1) setSelectedConnection(conns[0]);
      })
      .catch(() => {});
  }, [open]);

  // Load buckets when connection changes
  const loadBuckets = useCallback(async () => {
    if (!selectedConnection) {
      setBuckets([]);
      return;
    }
    try {
      const b = await storageListBuckets(selectedConnection);
      setBuckets(b);
      if (b.length > 0 && !b.some((bk) => bk.name === selectedBucket)) {
        setSelectedBucket(b[0].name);
      }
    } catch {
      setBuckets([]);
    }
  }, [selectedConnection, selectedBucket]);

  useEffect(() => {
    loadBuckets();
  }, [loadBuckets]);

  // Reset state on open
  useEffect(() => {
    if (open) {
      setKey("");
      setFormat("csv");
      setSaving(false);
    }
  }, [open]);

  // Set default key with extension
  useEffect(() => {
    if (open && !key) {
      const ext = format === "xlsx" ? "xlsx" : format;
      setKey(`results.${ext}`);
    }
  }, [open, format]); // eslint-disable-line react-hooks/exhaustive-deps

  // Update extension when format changes
  const handleFormatChange = (newFormat: string) => {
    setFormat(newFormat);
    // Update extension on key if it ends with a known extension
    const knownExts = [".csv", ".json", ".xlsx", ".parquet"];
    const currentKey = key;
    for (const ext of knownExts) {
      if (currentKey.endsWith(ext)) {
        setKey(currentKey.slice(0, -ext.length) + "." + newFormat);
        return;
      }
    }
  };

  const handleSave = async () => {
    if (!selectedConnection || !selectedBucket || !key.trim() || !query) return;
    setSaving(true);
    try {
      let result;
      if (mode === "workspace") {
        result = await workspaceExportToStorage({
          query,
          storage_connection: selectedConnection,
          bucket: selectedBucket,
          key: key.trim(),
          format,
        });
      } else {
        result = await storageExportQuery({
          connection,
          database,
          query,
          storage_connection: selectedConnection,
          bucket: selectedBucket,
          key: key.trim(),
          format,
        });
      }
      onSuccess?.(result);
      onClose();
    } catch (e) {
      onError?.(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const formats = mode === "workspace"
    ? [
        { value: "csv", label: "CSV (.csv)" },
        { value: "json", label: "JSON (.json)" },
        { value: "parquet", label: "Parquet (.parquet)" },
      ]
    : [
        { value: "csv", label: "CSV (.csv)" },
        { value: "json", label: "JSON (.json)" },
        { value: "xlsx", label: "Excel (.xlsx)" },
      ];

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Save to Storage</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label>Storage Connection</Label>
            <Select value={selectedConnection} onValueChange={setSelectedConnection}>
              <SelectTrigger>
                <SelectValue placeholder="Select connection..." />
              </SelectTrigger>
              <SelectContent>
                {storageConnections.map((c) => (
                  <SelectItem key={c} value={c}>{c}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label>Bucket</Label>
            <Select value={selectedBucket} onValueChange={setSelectedBucket}>
              <SelectTrigger>
                <SelectValue placeholder="Select bucket..." />
              </SelectTrigger>
              <SelectContent>
                {buckets.map((b) => (
                  <SelectItem key={b.name} value={b.name}>{b.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label>Object Key (path)</Label>
            <Input
              value={key}
              onChange={(e) => setKey(e.target.value)}
              placeholder="exports/results.csv"
            />
          </div>

          <div className="space-y-2">
            <Label>Format</Label>
            <Select value={format} onValueChange={handleFormatChange}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {formats.map((f) => (
                  <SelectItem key={f.value} value={f.value}>{f.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button
            onClick={handleSave}
            disabled={saving || !selectedConnection || !selectedBucket || !key.trim()}
          >
            {saving ? "Saving..." : "Save to Storage"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
