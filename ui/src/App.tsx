import { BrowserRouter, Routes, Route } from "react-router-dom";
import { AuthProvider, useAuth } from "./lib/auth";
import Login from "./components/Login";
import SetupWizard from "./components/SetupWizard";
import Layout from "./components/Layout";
import QueryPage from "./pages/QueryPage";
import TablesPage from "./pages/TablesPage";
import AdminPage from "./pages/AdminPage";
import MyAccessPage from "./pages/MyAccessPage";
import RealtimePage from "./pages/RealtimePage";
import ImportPage from "./pages/ImportPage";
import WorkspacePage from "./pages/WorkspacePage";
import MonitorPage from "./pages/MonitorPage";
import HealthPage from "./pages/HealthPage";
import StoragePage from "./pages/StoragePage";
import ObjectsPage from "./pages/ObjectsPage";
import ApprovalsPage from "./pages/ApprovalsPage";

function AppContent() {
  const { loading, needsSetup, authenticated, refreshStatus } = useAuth();

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <p className="text-muted-foreground">Loading...</p>
      </div>
    );
  }

  if (needsSetup) {
    return <SetupWizard onComplete={refreshStatus} />;
  }

  if (!authenticated) {
    return <Login />;
  }

  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<QueryPage />} />
          <Route path="/tables" element={<TablesPage />} />
          <Route path="/objects" element={<ObjectsPage />} />
          <Route path="/realtime" element={<RealtimePage />} />
          <Route path="/import" element={<ImportPage />} />
          <Route path="/workspace" element={<WorkspacePage />} />
          <Route path="/monitor" element={<MonitorPage />} />
          <Route path="/health" element={<HealthPage />} />
          <Route path="/storage" element={<StoragePage />} />
          <Route path="/access" element={<MyAccessPage />} />
          <Route path="/approvals" element={<ApprovalsPage />} />
          <Route path="/admin" element={<AdminPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <AppContent />
    </AuthProvider>
  );
}
