import { useState, useEffect, useCallback, useRef } from "react";
import {
  storageListConnections,
  storageListBuckets,
  storageCreateBucket,
  storageDeleteBucket,
  storageListObjects,
  storageUploadObject,
  storageDownloadObject,
  storageDeleteObject,
  storagePreview,
  storageImportToWorkspace,
} from "../lib/api";
import type { BucketInfo, ObjectInfo } from "../lib/api";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle,
  DialogFooter, DialogDescription,
} from "@/components/ui/dialog";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";

function formatSize(bytes: number): string {
  if (bytes === 0) return "-";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

const PREVIEWABLE_EXTENSIONS = ["csv", "tsv", "parquet", "json", "jsonl", "ndjson", "xlsx", "xls"];

function isPreviewable(key: string): boolean {
  const ext = key.split(".").pop()?.toLowerCase() ?? "";
  return PREVIEWABLE_EXTENSIONS.includes(ext);
}

export default function StoragePage() {
  const [connections, setConnections] = useState<string[]>([]);
  const [selectedConnection, setSelectedConnection] = useState<string>("");
  const [buckets, setBuckets] = useState<BucketInfo[]>([]);
  const [selectedBucket, setSelectedBucket] = useState<string>("");
  const [objects, setObjects] = useState<ObjectInfo[]>([]);
  const [prefix, setPrefix] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);

  // Dialogs
  const [showCreateBucket, setShowCreateBucket] = useState(false);
  const [deleteBucketTarget, setDeleteBucketTarget] = useState<string | null>(null);
  const [deleteObjectTarget, setDeleteObjectTarget] = useState<string | null>(null);

  // Upload
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [uploading, setUploading] = useState(false);

  // Load connections
  useEffect(() => {
    storageListConnections()
      .then((conns) => {
        setConnections(conns);
        if (conns.length === 1) setSelectedConnection(conns[0]);
      })
      .catch((e) => setError(e.message));
  }, []);

  // Load buckets when connection changes
  const refreshBuckets = useCallback(async () => {
    if (!selectedConnection) { setBuckets([]); return; }
    try {
      const b = await storageListBuckets(selectedConnection);
      setBuckets(b);
      if (b.length > 0 && !b.some((bk) => bk.name === selectedBucket)) {
        setSelectedBucket(b[0].name);
        setPrefix("");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [selectedConnection, selectedBucket]);

  useEffect(() => { refreshBuckets(); }, [refreshBuckets]);

  // Load objects when bucket or prefix changes
  const refreshObjects = useCallback(async () => {
    if (!selectedConnection || !selectedBucket) { setObjects([]); return; }
    setLoading(true);
    try {
      const objs = await storageListObjects(selectedConnection, selectedBucket, prefix || undefined);
      setObjects(objs);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [selectedConnection, selectedBucket, prefix]);

  useEffect(() => { refreshObjects(); }, [refreshObjects]);

  // Breadcrumbs from prefix
  const prefixParts = prefix ? prefix.split("/").filter(Boolean) : [];
  const breadcrumbs = [
    { label: selectedBucket || "root", prefix: "" },
    ...prefixParts.map((part, i) => ({
      label: part,
      prefix: prefixParts.slice(0, i + 1).join("/") + "/",
    })),
  ];

  const handleNavigate = (newPrefix: string) => {
    setPrefix(newPrefix);
  };

  const handleDownload = async (key: string) => {
    try {
      const blob = await storageDownloadObject(selectedConnection, selectedBucket, key);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = key.split("/").pop() || key;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleUpload = async (files: FileList | null) => {
    if (!files || files.length === 0 || !selectedConnection || !selectedBucket) return;
    setUploading(true);
    setError(null);
    try {
      for (const file of Array.from(files)) {
        const key = prefix ? `${prefix}${file.name}` : file.name;
        await storageUploadObject(selectedConnection, selectedBucket, file, key);
      }
      setInfo(`Uploaded ${files.length} file(s)`);
      refreshObjects();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  };

  const handleDeleteObject = async () => {
    if (!deleteObjectTarget) return;
    try {
      await storageDeleteObject(selectedConnection, selectedBucket, deleteObjectTarget);
      setDeleteObjectTarget(null);
      refreshObjects();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleDeleteBucket = async () => {
    if (!deleteBucketTarget) return;
    try {
      await storageDeleteBucket(selectedConnection, deleteBucketTarget);
      setDeleteBucketTarget(null);
      if (selectedBucket === deleteBucketTarget) {
        setSelectedBucket("");
        setPrefix("");
      }
      refreshBuckets();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handlePreview = async (key: string) => {
    setInfo(null);
    setError(null);
    try {
      const result = await storagePreview(selectedConnection, selectedBucket, key);
      setInfo(`Loaded into workspace as "${result.table_name}" (${result.row_count} rows). Go to Workspace tab to query.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleImportToWorkspace = async (key: string) => {
    setInfo(null);
    setError(null);
    try {
      const result = await storageImportToWorkspace({
        connection: selectedConnection,
        bucket: selectedBucket,
        key,
      });
      setInfo(`Imported into workspace as "${result.table_name}" (${result.row_count} rows). Go to Workspace tab to query.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    handleUpload(e.dataTransfer.files);
  };

  if (connections.length === 0) {
    return (
      <div className="h-full flex flex-col p-4 gap-4">
        <h2 className="text-xl font-bold">Storage</h2>
        <Card>
          <CardContent className="py-8 text-center text-muted-foreground">
            No storage connections configured. Add a MinIO/S3 connection in Admin &rarr; Connections.
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col p-4 gap-4">
      <h2 className="text-xl font-bold">Storage</h2>

      {error && (
        <div className="bg-destructive/20 border border-destructive text-destructive px-4 py-2 rounded-md text-sm">
          {error}
          <button className="ml-2 underline" onClick={() => setError(null)}>dismiss</button>
        </div>
      )}
      {info && (
        <div className="bg-green-500/20 border border-green-500 text-green-400 px-4 py-2 rounded-md text-sm">
          {info}
          <button className="ml-2 underline" onClick={() => setInfo(null)}>dismiss</button>
        </div>
      )}

      {/* Controls */}
      <div className="flex items-end gap-4 flex-wrap">
        <div className="space-y-1">
          <Label className="text-xs">Connection</Label>
          <Select value={selectedConnection} onValueChange={(v) => { setSelectedConnection(v); setSelectedBucket(""); setPrefix(""); }}>
            <SelectTrigger className="w-[200px]"><SelectValue placeholder="Select..." /></SelectTrigger>
            <SelectContent>
              {connections.map((c) => (
                <SelectItem key={c} value={c}>{c}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {selectedConnection && (
          <div className="space-y-1">
            <Label className="text-xs">Bucket</Label>
            <div className="flex gap-2">
              <Select value={selectedBucket} onValueChange={(v) => { setSelectedBucket(v); setPrefix(""); }}>
                <SelectTrigger className="w-[200px]"><SelectValue placeholder="Select bucket..." /></SelectTrigger>
                <SelectContent>
                  {buckets.map((b) => (
                    <SelectItem key={b.name} value={b.name}>{b.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button variant="outline" size="sm" onClick={() => setShowCreateBucket(true)}>New</Button>
              {selectedBucket && (
                <Button variant="outline" size="sm" className="text-destructive" onClick={() => setDeleteBucketTarget(selectedBucket)}>
                  Delete
                </Button>
              )}
            </div>
          </div>
        )}

        {selectedBucket && (
          <div className="flex gap-2 ml-auto">
            <input
              ref={fileInputRef}
              type="file"
              multiple
              className="hidden"
              onChange={(e) => handleUpload(e.target.files)}
            />
            <Button variant="outline" size="sm" onClick={() => fileInputRef.current?.click()} disabled={uploading}>
              {uploading ? "Uploading..." : "Upload File"}
            </Button>
            <Button variant="outline" size="sm" onClick={refreshObjects}>Refresh</Button>
          </div>
        )}
      </div>

      {/* Breadcrumbs */}
      {selectedBucket && (
        <div className="flex items-center gap-1 text-sm">
          {breadcrumbs.map((bc, i) => (
            <span key={bc.prefix}>
              {i > 0 && <span className="text-muted-foreground mx-1">/</span>}
              <button
                className="text-blue-400 hover:underline"
                onClick={() => handleNavigate(bc.prefix)}
              >
                {bc.label}
              </button>
            </span>
          ))}
        </div>
      )}

      {/* Objects table */}
      {selectedBucket && (
        <Card
          className="flex-1"
          onDragOver={(e) => e.preventDefault()}
          onDrop={handleDrop}
        >
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-8"></TableHead>
                  <TableHead>Name</TableHead>
                  <TableHead className="w-[100px]">Size</TableHead>
                  <TableHead className="w-[180px]">Last Modified</TableHead>
                  <TableHead className="w-[240px]">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading ? (
                  <TableRow>
                    <TableCell colSpan={5} className="text-center text-muted-foreground py-8">Loading...</TableCell>
                  </TableRow>
                ) : objects.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
                      {prefix ? "Empty folder" : "No objects in this bucket"}.
                      Drag & drop files here to upload.
                    </TableCell>
                  </TableRow>
                ) : objects.map((obj) => {
                  const displayName = obj.is_prefix
                    ? obj.key.replace(prefix, "").replace(/\/$/, "")
                    : obj.key.replace(prefix, "");
                  return (
                    <TableRow key={obj.key}>
                      <TableCell className="text-center">
                        {obj.is_prefix ? (
                          <span title="Folder" className="text-yellow-400">&#128193;</span>
                        ) : (
                          <span title="File" className="text-muted-foreground">&#128196;</span>
                        )}
                      </TableCell>
                      <TableCell>
                        {obj.is_prefix ? (
                          <button
                            className="text-blue-400 hover:underline font-mono text-xs"
                            onClick={() => handleNavigate(obj.key)}
                          >
                            {displayName}/
                          </button>
                        ) : (
                          <span className="font-mono text-xs">{displayName}</span>
                        )}
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {obj.is_prefix ? "-" : formatSize(obj.size)}
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {obj.last_modified ?? "-"}
                      </TableCell>
                      <TableCell className="space-x-1">
                        {!obj.is_prefix && (
                          <>
                            <Button variant="ghost" size="sm" onClick={() => handleDownload(obj.key)}>
                              Download
                            </Button>
                            {isPreviewable(obj.key) && (
                              <>
                                <Button variant="ghost" size="sm" onClick={() => handlePreview(obj.key)}>
                                  Preview
                                </Button>
                                <Button variant="ghost" size="sm" onClick={() => handleImportToWorkspace(obj.key)}>
                                  Import
                                </Button>
                              </>
                            )}
                            <Button variant="ghost" size="sm" className="text-destructive" onClick={() => setDeleteObjectTarget(obj.key)}>
                              Delete
                            </Button>
                          </>
                        )}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* Create Bucket Dialog */}
      <CreateBucketDialog
        open={showCreateBucket}
        onClose={() => setShowCreateBucket(false)}
        onCreate={async (name) => {
          await storageCreateBucket(selectedConnection, name);
          refreshBuckets();
        }}
        onError={setError}
      />

      {/* Delete Bucket Confirm */}
      <Dialog open={!!deleteBucketTarget} onOpenChange={(v) => !v && setDeleteBucketTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Bucket</DialogTitle>
            <DialogDescription>
              This will permanently delete the bucket <span className="font-mono">"{deleteBucketTarget}"</span>.
              The bucket must be empty.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteBucketTarget(null)}>Cancel</Button>
            <Button variant="destructive" onClick={handleDeleteBucket}>Delete</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Object Confirm */}
      <Dialog open={!!deleteObjectTarget} onOpenChange={(v) => !v && setDeleteObjectTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Object</DialogTitle>
            <DialogDescription>
              This will permanently delete <span className="font-mono">"{deleteObjectTarget}"</span>.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteObjectTarget(null)}>Cancel</Button>
            <Button variant="destructive" onClick={handleDeleteObject}>Delete</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function CreateBucketDialog({ open, onClose, onCreate, onError }: {
  open: boolean;
  onClose: () => void;
  onCreate: (name: string) => Promise<void>;
  onError: (msg: string) => void;
}) {
  const [name, setName] = useState("");
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    if (open) setName("");
  }, [open]);

  const submit = async () => {
    if (!name.trim()) return;
    setCreating(true);
    try {
      await onCreate(name.trim());
      onClose();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-sm">
        <DialogHeader><DialogTitle>Create Bucket</DialogTitle></DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label>Bucket Name</Label>
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-bucket"
              onKeyDown={(e) => e.key === "Enter" && submit()}
            />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={creating || !name.trim()}>
            {creating ? "Creating..." : "Create"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
