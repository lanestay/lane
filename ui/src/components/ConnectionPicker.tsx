import { useEffect, useState } from "react";
import { listConnections, listDatabases } from "../lib/api";
import type { ConnectionInfo, DatabaseInfo } from "../lib/api";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface Props {
  connection: string;
  database: string;
  onConnectionChange: (name: string, defaultDb: string, connType: string) => void;
  onDatabaseChange: (name: string) => void;
}

function StatusDot({ status }: { status: string }) {
  const color =
    status === "connected"
      ? "bg-green-500"
      : status === "error"
        ? "bg-red-500"
        : "bg-gray-400";
  return <span className={`inline-block w-2 h-2 rounded-full ${color} mr-1.5 shrink-0`} />;
}

export default function ConnectionPicker({ connection, database, onConnectionChange, onDatabaseChange }: Props) {
  const [connections, setConnections] = useState<ConnectionInfo[]>([]);
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);

  useEffect(() => {
    listConnections().then((conns) => {
      setConnections(conns);
      if (!connection && conns.length > 0) {
        const def = conns.find((c) => c.is_default) ?? conns[0];
        onConnectionChange(def.name, def.default_database, def.type);
      }
    });
  }, []);

  useEffect(() => {
    if (!connection) return;
    listDatabases(connection).then(setDatabases);
  }, [connection]);

  return (
    <div className="flex items-center gap-3">
      <div className="flex items-center gap-2">
        <label className="text-sm text-muted-foreground">Connection:</label>
        <Select
          value={connection}
          onValueChange={(name) => {
            const conn = connections.find((c) => c.name === name);
            onConnectionChange(name, conn?.default_database ?? "", conn?.type ?? "mssql");
          }}
        >
          <SelectTrigger className="w-[200px]">
            <SelectValue placeholder="Select connection" />
          </SelectTrigger>
          <SelectContent>
            {connections.map((c) => (
              <SelectItem key={c.name} value={c.name}>
                <span className="flex items-center">
                  <StatusDot status={c.status} />
                  {c.name} ({c.type})
                </span>
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="flex items-center gap-2">
        <label className="text-sm text-muted-foreground">Database:</label>
        <Select value={database} onValueChange={onDatabaseChange}>
          <SelectTrigger className="w-[200px]">
            <SelectValue placeholder="Select database" />
          </SelectTrigger>
          <SelectContent>
            {databases.map((d) => (
              <SelectItem key={d.name} value={d.name}>
                {d.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    </div>
  );
}
